plugins {
    id("java-library")
}

group = "net.momirealms"
version = "1.0.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    // Netty 由 Lettuce 传递进入编译类路径, 运行时由使用方的平台提供.
    compileOnly(libs.lettuce.core)
    compileOnly(libs.jetbrains.annotations)
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}