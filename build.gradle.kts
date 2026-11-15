plugins {
    id("java-library")
    id("maven-publish")
}

group = "net.momirealms"
version = "26.11"

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

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "sparrow-message"
            from(components["java"])
            version = project.version.toString()
            pom {
                name = "Sparrow Message"
                url = "https://github.com/NanamoServer/sparrow-message"
            }
        }
    }

    repositories {
        maven {
            name = "XiaoMoMi"
            url = uri("https://repo.momirealms.net/releases")
            credentials(PasswordCredentials::class)
            authentication {
                create<BasicAuthentication>("basic")
            }
        }
    }
}