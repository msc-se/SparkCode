@file:Suppress("SqlResolve", "SqlNoDataSourceInspection")

package com.sdu.big.data.covid

import org.apache.kafka.common.serialization.StringDeserializer
import org.jetbrains.kotlinx.spark.api.*
import kotlin.collections.HashMap


const val checkpoint = "hdfs://node-master:9000/user/hadoop/processed-tweets-offset"

val kafkaParams: HashMap<String, String> = hashMapOf(
    "kafka.bootstrap.servers" to "node-master:9092",
    "subscribe" to "processed-tweets",
    "group.id" to "spark"
)

val topics = listOf("processed-tweets")

const val MAPPER_HDFS_PATH = "hdfs://node-master:9000/user/hadoop/covid19-country-lookup/country-lookup.csv"

const val CASES_HDFS_PATH = "hdfs://node-master:9000/user/hadoop/covid19-cases/%s.csv"

fun main() {

    withSpark(appName = "Covid Processor", props = mapOf("spark.sql.shuffle.partitions" to "5")) {

        //kafkaParams["startingOffsets"] = "earliest"
        //kafkaParams["endingOffsets"] = "latest"

        var kafkaDf = spark
            .read()
            .format("kafka")
            .options(kafkaParams)
            .load()

        kafkaDf = kafkaDf.selectExpr("CAST(key as STRING)", "CAST(value as STRING)")


        val x = kafkaDf.groupByKey { row -> row.getString(0) }.mapValues { row -> row.getString(1).toLong() }.reduceGroups(func = { x, y -> x + y })


/*
        // Read files
        val covidStream = spark.read().option("header", true)
            .csv(CASES_HDFS_PATH.format("12-06-2020"))
        val oldCovidStream = spark.read().option("header", true)
            .csv(CASES_HDFS_PATH.format("12-05-2020"))
        val mapper = spark.read().option("header", true).csv(MAPPER_HDFS_PATH)

        // Create tables to be able to use SQL
        covidStream.createOrReplaceTempView("covid")
        oldCovidStream.createOrReplaceTempView("old_covid")

        // Combine the two tables and count total confirmed and difference between yesterday and today
        val covid = spark.sql("SELECT covid.Country_Region AS country, SUM(CAST(covid.Confirmed AS BIGINT)) AS cases, SUM(CAST(covid.Confirmed AS BIGINT) - CAST(old_covid.Confirmed AS BIGINT)) AS new_cases FROM covid INNER JOIN old_covid ON covid.Combined_Key = old_covid.Combined_Key GROUP BY covid.Country_Region")

        // Update the SQL view
        covid.createOrReplaceTempView("covid")

        // Filter mapper csv so that only one row per country and create a SQL view
        mapper.filter { row -> row.isNullAt(6) }.createOrReplaceTempView("mapper")

        // Replace country name with iso2 code and add population
        val newResult =
            spark.sql("SELECT iso2 as code, cases, new_cases, population FROM covid INNER JOIN mapper ON covid.country = mapper.Country_Region")
*/
        // Write the result
        val query = x.write()
            .format("console")
            .save()
    }
}
