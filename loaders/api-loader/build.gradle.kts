plugins {
    id("asp.base-conventions")
    id("asp.publishing-conventions")
}

dependencies {
    compileOnly(project(":slime-api"))
    compileOnly(paperApi())
}

publishConfiguration {
    name = "Advanced Slime Paper API loader"
    description = "HTTP-API based loader for Advanced Slime Paper"
}
