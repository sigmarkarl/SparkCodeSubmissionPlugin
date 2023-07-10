plugins {
    id("java")
    id("application")
    id("maven-publish")
}

group = "com.netapp.spark"
version = "1.0.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

var theJvmArgs = listOf(
    "--enable-preview",
    /*"--add-opens=java.base/java.util.regex=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.time=ALL-UNNAMED",
    "--add-opens=java.base/java.util.stream=ALL-UNNAMED",
    "--add-opens=java.base/java.nio.charset=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED"*/
)

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.4.1")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

application {
    mainClass.set("com.netapp.spark.SparkConnectWebsocketTranscodeDriverPlugin")
    //applicationArguments = listOf("9001", "local[*]")
    applicationDefaultJvmArgs = theJvmArgs
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.netapp.spark"
            artifactId = "codesubmission-client"
            version = "1.0.0"

            from(components["java"])
        }
    }
    repositories {
        maven {
            url = uri(layout.projectDirectory.dir("../repo"))
        }
        /*maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/sigmarkarl/SparkCodeSubmissionPlugin")
            credentials {
                username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
                password = project.findProperty("gpr.key") as String? ?: System.getenv("TOKEN")
            }
        }*/
    }
}