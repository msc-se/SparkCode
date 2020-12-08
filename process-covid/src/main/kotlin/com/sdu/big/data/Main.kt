@file:Suppress("SqlResolve", "SqlNoDataSourceInspection")

package com.sdu.big.data.covid

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.jetbrains.kotlinx.spark.api.withSpark
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset.UTC
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*


const val checkpoint = "hdfs://node-master:9000/user/hadoop/processed-tweets-offset"

val kafkaParams: HashMap<String, Any> = hashMapOf(
    "bootstrap.servers" to "node-master:9092",
    "key.deserializer" to StringDeserializer::class.java.name,
    "value.deserializer" to StringDeserializer::class.java.name,
    "group.id" to "spark"
)

val topics = listOf("processed-tweets")

const val MAPPER_HDFS_PATH = "hdfs://node-master:9000/user/hadoop/covid19-country-lookup/country-lookup.csv"

const val CASES_HDFS_PATH = "hdfs://node-master:9000/user/hadoop/covid19-cases/%s.csv"

const val KAFKA_TOPIC = "processed-tweets"

const val OFFSET = """ {"%s":{"0": %s}} """

fun main() {
    val logger = LogManager.getRootLogger()
    logger.level = Level.ERROR

    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "node-master:9092"
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

    withSpark(appName = "Covid Processor", props = mapOf("spark.sql.shuffle.partitions" to "5")) {
        //val date = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("MM-dd-yyyy"))
        val date = "12-08-2020"

        // Read files
        val covidStream = spark.read().option("header", true)
            .csv(CASES_HDFS_PATH.format("12-06-2020"))
        val oldCovidStream = spark.read().option("header", true)
            .csv(CASES_HDFS_PATH.format("12-05-2020"))
        val mapper = spark.read().option("header", true).csv(MAPPER_HDFS_PATH)

        val sdf = SimpleDateFormat("MM-dd-yyyy HH:mm")
        val startOffsetTime = sdf.parse("%s 00:00".format(date)).time
        val endingOffsetTime = sdf.parse("%s 23:59".format(date)).time

        val tweets = spark
            .read()
            .format("kafka")
            .option("kafka.bootstrap.servers", "node-master:9092")
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsetsByTimestamp", OFFSET.format(KAFKA_TOPIC, startOffsetTime))
            .option("endingOffsetsByTimestamp", OFFSET.format(KAFKA_TOPIC, endingOffsetTime))
            .load()

        // Create tables to be able to use SQL
        covidStream.createOrReplaceTempView("covid")
        oldCovidStream.createOrReplaceTempView("old_covid")

        // Combine the two tables and count total confirmed and difference between yesterday and today
        val covid = spark.sql("SELECT covid.Country_Region AS country, SUM(CAST(covid.Confirmed AS LONG)) AS cases, SUM(CAST(covid.Confirmed AS LONG) - CAST(old_covid.Confirmed AS LONG)) AS new_cases FROM covid INNER JOIN old_covid ON covid.Combined_Key = old_covid.Combined_Key GROUP BY covid.Country_Region")

        // Update the SQL view
        covid.createOrReplaceTempView("covid")

        // Filter mapper csv so that only one row per country and create a SQL view
        mapper.filter { row -> row.isNullAt(6) }.createOrReplaceTempView("mapper")

        // Replace country name with iso2 code and add population
        val newResult =
            spark.sql("SELECT iso2 as code, cases, new_cases, population FROM covid INNER JOIN mapper ON covid.country = mapper.Country_Region")

        // Write the result
        val query = tweets.write()
            .format("console")
            .save()
    }
}
