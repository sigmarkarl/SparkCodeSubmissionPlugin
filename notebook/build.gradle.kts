plugins {
    id("java-library")
    id("maven-publish")
    id("com.google.cloud.tools.jib") version "3.3.2"
}

group = "com.netapp.spark"
version = "1.0.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.netapp.spark"
            artifactId = "codesubmission-notebook"
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

jib {
    from {
        image = "openjdk:21-jdk"
        //image = "public.ecr.aws/l8m2k1n1/netapp/spark/codesubmission:baseimage-1.0.0"
        //version = "baseimage-1.0.0"
        platforms {
            platform {
                architecture = "amd64"
                os = "linux"
            }
            /*platform {
                architecture = "arm64"
                os = "linux"
            }*/
        }
        if (project.hasProperty("REGISTRY_USER")) {
            auth {
                username = project.findProperty("REGISTRY_USER")?.toString()
                password = project.findProperty("REGISTRY_PASSWORD")?.toString()
            }
        }
    }
    to {
        image = project.findProperty("APPLICATION_REPOSITORY")?.toString() ?: "public.ecr.aws/l8m2k1n1/netapp/spark/notebookinit:1.0.0"
        //version = "1.0.0"
        //tags = [project.findProperty("APPLICATION_TAG")?.toString() ?: "1.0"]
        if (project.hasProperty("REGISTRY_USER")) {
            val reg_user = project.findProperty("REGISTRY_USER")?.toString()
            val reg_pass = project.findProperty("REGISTRY_PASSWORD")?.toString()
            auth {
                username = reg_user
                password = reg_pass
            }
        }
    }
    //containerizingMode = "packaged"
    container {
        mainClass = "com.netapp.spark.NotebookInitContainer"
        jvmFlags = theJvmArgs
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
    implementation("org.slf4j:slf4j-api:2.0.7")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}