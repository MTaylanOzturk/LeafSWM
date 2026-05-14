plugins {
    id("asp.base-conventions")
    id("asp.publishing-conventions")
}

dependencies {
    compileOnly(project(":slime-api"))

    api(libs.hikari)
    compileOnly(paperApi())
}

publishConfiguration {
    name = "Advanced Slime Paper MySQL Loader"
    description = "MySQL loader for Advanced Slime Paper"
}
