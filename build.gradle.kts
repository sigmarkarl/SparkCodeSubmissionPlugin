plugins {
    id("java-library")
    id("scala")
    id("application")
    id("maven-publish")
    id("org.graalvm.buildtools.native") version "0.9.21"
    id("com.google.cloud.tools.jib") version "3.3.1"
}

group = "com.netapp.spark"
version = "1.0.0"

var theJvmArgs = listOf(
    "--enable-preview",
    "--add-opens=java.base/java.util.regex=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.time=ALL-UNNAMED",
    "--add-opens=java.base/java.util.stream=ALL-UNNAMED",
    "--add-opens=java.base/java.nio.charset=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
)

/*sourceSets {
    main {
        withConvention(ScalaSourceSet::class) {
            scala {
                setSrcDirs(listOf("src/main/scala"))
            }
        }
    }
}*/

repositories {
    mavenCentral()
}

jib {
    from {
        image = "public.ecr.aws/l8m2k1n1/netapp/spark/codesubmission:baseimage-1.0.0"
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
    //containerizingMode = "packaged"
    container {
        //user = "app:app"
        //entrypoint = listOf("/opt/entrypoint.sh")
        //workingDirectory = "/opt/spark/work-dir/"
        //appRoot = "/opt/spark/"
        mainClass = "com.netapp.spark.SparkCodeSubmissionServer"
        //environment = mapOf("SPARK_REMOTE" to "sc://localhost")
        //environment = mapOf("JAVA_TOOL_OPTIONS" to "-Djdk.lang.processReaperUseDefaultStackSize=true --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED")
        jvmFlags = theJvmArgs
        args = listOf("9001")
    }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(20))
    }
}

tasks.withType<ScalaCompile>().configureEach {
    scalaCompileOptions.additionalParameters = listOf("-target:jvm-1.8")
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("--enable-preview")
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.4.0")
    implementation("org.apache.spark:spark-sql_2.12:3.4.0")
    implementation("org.apache.spark:spark-connect_2.12:3.4.0")
    implementation("org.apache.spark:spark-connect-client-jvm_2.12:3.4.0")
    implementation("io.undertow:undertow-core:2.3.5.Final")
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

application {
    mainClass.set("com.netapp.spark.SparkCodeSubmissionServer")
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
            mainClass.set("com.netapp.spark.SparkCodeSubmissionServer")
            jvmArgs.addAll(theJvmArgs)
            runtimeArgs.addAll(listOf("9001"))
            useFatJar.set(true)
            //zip64.set(true)
        }
    }
}