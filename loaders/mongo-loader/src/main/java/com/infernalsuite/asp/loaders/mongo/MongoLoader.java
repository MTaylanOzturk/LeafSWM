package com.infernalsuite.asp.loaders.mongo;

import com.infernalsuite.asp.api.exceptions.UnknownWorldException;
import com.infernalsuite.asp.api.loaders.UpdatableLoader;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.*;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MongoLoader extends UpdatableLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoLoader.class);

    private final MongoClient client;
    private final String database;
    private final String collection;

    // Dünya başına kilit: aynı dünyanın eşzamanlı kayıt/sil işlemlerini sıraya sokar. Aksi halde
    // ASP'nin iç (async) autosave'i ile bir başka kayıt aynı GridFS dosyasında çakışıp
    // "No file found with the id" hatasına, mükerrer dosyalara ve okumada yanlış revizyonun
    // seçilmesine (dünyanın eski hâle dönmesi) yol açıyordu. Kilit dünya başına olduğundan
    // farklı dünyalar paralel kaydedilmeye devam eder.
    private final ConcurrentHashMap<String, Object> worldLocks = new ConcurrentHashMap<>();

    private Object lockFor(String worldName) {
        return worldLocks.computeIfAbsent(worldName, k -> new Object());
    }

    public MongoLoader(String database, String collection, @Nullable String username, @Nullable String password,
                       @Nullable String authSource, @Nullable String host, @Nullable Integer port, @Nullable String uri) throws MongoException {
        this.database = database;
        this.collection = collection;

        String authParams = username != null && password != null ? username + ":" + password + "@" : "";
        String parsedAuthSource = authSource != null ? "/?authSource=" + authSource : "";
        String parsedUri = uri != null && !uri.isBlank() ? uri : "mongodb://" + authParams + host + ":" + port + parsedAuthSource;

        this.client = MongoClients.create(parsedUri);

        init();
    }

    public MongoLoader(MongoClient client, String database, String collection) {
        this.database = database;
        this.collection = collection;
        this.client = client;

        init();
    }

    private void init() {
        MongoDatabase mongoDatabase = client.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        mongoCollection.createIndex(Indexes.ascending("name"), new IndexOptions().unique(true));
    }

    @Override
    public void update() {
        MongoDatabase mongoDatabase = client.getDatabase(database);

        // Old GridFS importing
        for (String collectionName : mongoDatabase.listCollectionNames()) {
            if (collectionName.equals(collection + "_files.files") || collectionName.equals(collection + "_files.chunks")) {
                LOGGER.info("Updating MongoDB database...");

                mongoDatabase.getCollection(collection + "_files.files").renameCollection(new MongoNamespace(database, collection + ".files"));
                mongoDatabase.getCollection(collection + "_files.chunks").renameCollection(new MongoNamespace(database, collection + ".chunks"));

                LOGGER.info("MongoDB database updated!");
                break;
            }
        }

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

        // Old world lock importing
        try (MongoCursor<Document> documents = mongoCollection.find(Filters.or(Filters.eq("locked", true),
                Filters.eq("locked", false))).cursor()) {
            if (documents.hasNext()) {
                LOGGER.warn("Your SWM MongoDB database is outdated. The update process will start in 10 seconds.");
                LOGGER.warn("Note that this update will make your database incompatible with older SWM versions.");
                LOGGER.warn("Make sure no other servers with older SWM versions are using this database.");
                LOGGER.warn("Shut down the server to prevent your database from being updated.");

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    LOGGER.info("Update process aborted.");
                    return;
                }

                while (documents.hasNext()) {
                    String name = documents.next().getString("name");
                    mongoCollection.updateOne(Filters.eq("name", name), Updates.set("locked", 0L));
                }
            }
        }
    }

    @Override
    public byte[] readWorld(String worldName) throws UnknownWorldException, IOException {
        try {
            MongoDatabase mongoDatabase = client.getDatabase(database);
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
            Document worldDoc = mongoCollection.find(Filters.eq("name", worldName)).first();

            if (worldDoc == null) {
                throw new UnknownWorldException(worldName);
            }

            GridFSBucket bucket = GridFSBuckets.create(mongoDatabase, collection);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            bucket.downloadToStream(worldName, stream);

            return stream.toByteArray();
        } catch (MongoException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public boolean worldExists(String worldName) throws IOException {
        try {
            MongoDatabase mongoDatabase = client.getDatabase(database);
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
            Document worldDoc = mongoCollection.find(Filters.eq("name", worldName)).first();

            return worldDoc != null;
        } catch (MongoException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public List<String> listWorlds() throws IOException {
        List<String> worldList = new ArrayList<>();

        try {
            MongoDatabase mongoDatabase = client.getDatabase(database);
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
            try (MongoCursor<Document> documents = mongoCollection.find().cursor()) {
                while (documents.hasNext()) {
                    worldList.add(documents.next().getString("name"));
                }
            }
        } catch (MongoException ex) {
            throw new IOException(ex);
        }

        return worldList;
    }

    @Override
    public void saveWorld(String worldName, byte[] serializedWorld) throws IOException {
        try {
            MongoDatabase mongoDatabase = client.getDatabase(database);
            GridFSBucket bucket = GridFSBuckets.create(mongoDatabase, collection);
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);

            // Aynı dünyanın eşzamanlı kayıtlarını sıraya sok (dünya başına kilit). Bu blok atomik
            // davranır: önce yeni revizyon yüklenir, sonra bu isimdeki DİĞER tüm revizyonlar silinir.
            synchronized (lockFor(worldName)) {
                // 1) Yeni revizyonu yükle. Yükleme bitene kadar eski dosya okunabilir kalır.
                ObjectId newId = bucket.uploadFromStream(worldName, new ByteArrayInputStream(serializedWorld));

                // 2) Bu isimdeki diğer tüm revizyonları (eski + önceki yarışlardan kalan yetim/mükerrer
                //    dosyalar) temizle → geriye tam olarak TEK dosya kalır, okuma daima belirleyicidir.
                for (GridFSFile file : bucket.find(Filters.eq("filename", worldName))) {
                    if (!file.getObjectId().equals(newId)) {
                        bucket.delete(file.getObjectId());
                    }
                }

                // 3) İsim indeksini upsert et (readWorld/worldExists bu dokümana bakar).
                mongoCollection.updateOne(
                        Filters.eq("name", worldName),
                        new Document("$set", new Document("name", worldName)),
                        new UpdateOptions().upsert(true)
                );
            }
        } catch (MongoException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void deleteWorld(String worldName) throws IOException, UnknownWorldException {
        try {
            synchronized (lockFor(worldName)) {
                MongoDatabase mongoDatabase = client.getDatabase(database);
                GridFSBucket bucket = GridFSBuckets.create(mongoDatabase, collection);

                // Bu isimdeki TÜM revizyonları sil (önceki yarışlardan kalan mükerrerler dahil),
                // sadece ilkini değil — aksi halde yetim dosyalar GridFS'te kalıcı olarak birikiyordu.
                boolean found = false;
                for (GridFSFile file : bucket.find(Filters.eq("filename", worldName))) {
                    bucket.delete(file.getObjectId());
                    found = true;
                }

                if (!found) {
                    throw new UnknownWorldException(worldName);
                }

                // Delete backup file
                for (GridFSFile backupFile : bucket.find(Filters.eq("filename", worldName + "_backup"))) {
                    bucket.delete(backupFile.getObjectId());
                }

                MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
                mongoCollection.deleteOne(Filters.eq("name", worldName));
            }
        } catch (MongoException ex) {
            throw new IOException(ex);
        }
    }

}
