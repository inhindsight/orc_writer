import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.50"
    application
}

repositories {
    mavenCentral()
}

application {
    mainClassName = "orsimer.AppKt"
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.erlang.otp:jinterface:1.6.1")
    implementation("org.apache.orc:orc:1.6.1")
    testImplementation("org.junit.jupiter:junit-jupiter:5.5.2")
    testImplementation("org.assertj:assertj-core:3.14.0")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
