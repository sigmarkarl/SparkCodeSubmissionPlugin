plugins {
    id("java-library")
    id("maven-publish")
}

group = "com.netapp.spark"
version = "1.0.0"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.3.2")
    implementation("org.apache.spark:spark-sql_2.12:3.3.2")
    implementation("io.dropwizard:dropwizard-core:4.0.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")

    testImplementation(platform("org.junit:junit-bom:5.9.2"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.netapp.spark"
            artifactId = "codesubmit"
            version = "1.0.0"

            from(components["java"])
        }
    }
    repositories {
        maven {
            url = uri(layout.projectDirectory.dir("repo"))
        }
    }
}

tasks.test {
    useJUnitPlatform()
}