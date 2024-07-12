plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    // https://mvnrepository.com/artifact/janino/janino
    implementation("org.codehaus.janino:janino:3.0.16")

    // https://mvnrepository.com/artifact/org.codehaus.janino/commons-compiler
    implementation("org.codehaus.janino:commons-compiler:3.0.16")

    implementation("org.apache.spark:spark-sql_2.12:3.0.0") {
        exclude(group = "org.codehaus.janino")
    }

    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
    implementation ("org.apache.spark:spark-core_2.12:3.0.0"){
        exclude(group = "org.codehaus.janino")
    }
}

tasks.test {
    useJUnitPlatform()
}


