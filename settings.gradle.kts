rootProject.name = "SparkCodeSubmissionPlugin"
rootProject.buildFileName = "build.gradle.kts"
include(":client",":server")

pluginManagement {
    repositories {
        mavenCentral()
        maven { url = uri("https://plugins.gradle.org/m2/")}
        gradlePluginPortal()
    }
}

buildscript {
    repositories {
        maven { url = uri("https://plugins.gradle.org/m2/")}
        mavenCentral()
        gradlePluginPortal()
    }
    dependencies {
        classpath("com.google.cloud.tools:jib-ownership-extension-gradle:0.1.0")
        classpath("com.google.cloud.tools:jib-native-image-extension-gradle:0.1.0")
    }
}
include("client")
include("server")
