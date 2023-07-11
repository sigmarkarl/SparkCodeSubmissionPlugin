plugins {
    id("java-library")
    id("application")
    id("maven-publish")
    id("org.graalvm.buildtools.native") version "0.9.22"
    id("com.google.cloud.tools.jib") version "3.3.2"
}

group = "com.netapp.spark"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.4.1") {
        exclude(group = "com.google.guava")
    }
    implementation("org.apache.spark:spark-sql_2.12:3.4.1") {
        exclude(group = "com.google.guava")
    }
    implementation("org.apache.spark:spark-connect_2.12:3.4.1")
    implementation("org.apache.spark:spark-repl_2.12:3.4.1")
    implementation("org.apache.spark:spark-hive_2.12:3.4.1")
    implementation("org.apache.spark:spark-hive-thriftserver_2.12:3.4.1")

    implementation("org.apache.arrow:flight-sql:12.0.1")
    implementation("org.apache.arrow:flight-sql-jdbc-core:12.0.1")
    implementation("org.apache.arrow:flight-core:12.0.1")
    implementation("org.apache.arrow:arrow-flight:12.0.1")
    implementation("org.apache.arrow:arrow-jdbc:12.0.1")

    implementation("io.undertow:undertow-core:2.3.5.Final")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")
    implementation("org.apache.commons:commons-compress:1.23.0")
    implementation("io.kubernetes:client-java:18.0.0")
    implementation("com.google.code.gson:gson:2.10.1")
    implementation("io.fabric8:kubernetes-client:6.7.2")
    implementation("org.eclipse.jgit:org.eclipse.jgit:6.6.0.202305301015-r")

    implementation("com.google.guava:guava:32.1.1-jre")
    implementation("com.google.guava:failureaccess:1.0.1")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
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
        image = "public.ecr.aws/l8m2k1n1/netapp/spark/codesubmission:0.9.9"
        //image = "public.ecr.aws/l8m2k1n1/netapp/spark/codesubmission:baseimage-1.0.0"
        //version = "baseimage-1.0.0"
        platforms {
            platform {
                architecture = "amd64"
                os = "linux"
            }
            platform {
                architecture = "arm64"
                os = "linux"
            }
        }
        if (project.hasProperty("REGISTRY_USER")) {
            auth {
                username = project.findProperty("REGISTRY_USER")?.toString()
                password = project.findProperty("REGISTRY_PASSWORD")?.toString()
            }
        }
    }
    to {
        image = project.findProperty("APPLICATION_REPOSITORY")?.toString() ?: "public.ecr.aws/l8m2k1n1/netapp/spark/codesubmission:1.0.0"
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
    /*pluginExtensions {
        pluginExtension {
            implementation = "com.google.cloud.tools.jib.gradle.extension.nativeimage.JibNativeImageExtension"
            properties = mapOf(
                "imageName" to "com.netapp.spark.NotebookInitContainer",
            )
        }
    }*/
//    pluginExtensions {
//        pluginExtension {
//            implementation = "com.google.cloud.tools.jib.gradle.extension.ownership.JibOwnershipExtension"
//            configuration {
//                "rules" to listOf(
//                        "rule" to mapOf(
//                                "filePattern" to "/opt/spark/**",
//                                "glob" to "/opt/spark/**",
//                                "ownership" to "app:app"
//                        )
//                )
//            }
//        }
//    }
    containerizingMode = "packaged"
    container {
        //user = "app:app"
        entrypoint = listOf("/opt/entrypoint.sh")
        workingDirectory = "/opt/spark/work-dir/"
        appRoot = "/opt/spark/"
        //mainClass = "com.netapp.spark.SparkCodeSubmissionServer"
        //mainClass = "com.netapp.spark.SparkConnectWebsocketTranscodeDriverPlugin"
        //environment = mapOf("SPARK_REMOTE" to "sc://localhost")
        //environment = mapOf("JAVA_TOOL_OPTIONS" to "-Djdk.lang.processReaperUseDefaultStackSize=true --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED")
        jvmFlags = theJvmArgs
        //args = listOf("9001")
    }
    /*extraDirectories {
        permissions = mapOf("/opt/entrypoint.sh" to "777")
    }*/
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.netapp.spark"
            artifactId = "codesubmission-server"
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

application {
    mainClass.set("com.netapp.spark.SparkCodeSubmissionServer")
    //mainClass.set("com.netapp.spark.SparkConnectWebsocketTranscodeDriverPlugin")
    //applicationArguments = listOf("9001", "local[*]")
    applicationDefaultJvmArgs = theJvmArgs
}

tasks {
    val fatJar = register<Jar>("fatJar") {
        setProperty("zip64",true)
        dependsOn.addAll(listOf("compileJava", "processResources")) // We need this for Gradle optimization to work
        archiveClassifier.set("sparkcodesubmissionplugin") // Naming the jar
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        //manifest { attributes(mapOf("Main-Class" to application.mainClass, "")) } // Provided we set it up in the application plugin configuration
        val sourcesMain = sourceSets.main.get()
        val contents = configurations.runtimeClasspath.get()
            .map { if (it.isDirectory) it else zipTree(it) } +
                sourcesMain.output
        from(contents)
    }
    build {
        dependsOn(fatJar) // Trigger fat jar creation during build
    }
}

tasks.test {
    jvmArgs = theJvmArgs
    useJUnitPlatform()
}

/*tasks.named<BuildNativeImageTask>("nativeCompile") {
    classpathJar.set(myFatJar.flatMap { it.archiveFile })
}*/

graalvmNative {
    binaries {
        named("main") {
            //mainClass.set("com.netapp.spark.SparkCodeSubmissionServer")
            mainClass.set("com.netapp.spark.NotebookInitContainer")
            jvmArgs.addAll(theJvmArgs)
            //runtimeArgs.addAll(listOf("9001"))
            useFatJar.set(true)
            //zip64.set(true)
        }
    }
}