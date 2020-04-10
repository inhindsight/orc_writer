import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.50"
    application
    idea
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
    implementation("org.apache.orc:orc-core:1.6.2")
    implementation("org.apache.hadoop:hadoop-common:3.2.1")
    implementation("org.apache.hadoop:hadoop-hdfs-client:3.2.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.2")
    testImplementation("org.junit.jupiter:junit-jupiter:5.5.2")
    testImplementation("org.assertj:assertj-core:3.14.0")
    testImplementation("org.awaitility:awaitility-kotlin:4.0.2")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showExceptions = true
        showCauses = true
    }
}
