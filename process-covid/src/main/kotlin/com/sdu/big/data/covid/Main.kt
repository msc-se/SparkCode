@file:Suppress("SqlResolve", "SqlNoDataSourceInspection")

package com.sdu.big.data.covid

import org.apache.spark.SparkConf
import org.jetbrains.kotlinx.spark.api.groupByKey
import org.jetbrains.kotlinx.spark.api.mapValues
import org.jetbrains.kotlinx.spark.api.withSpark
import org.jetbrains.kotlinx.spark.api.*
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

const val MAPPER_HDFS_PATH = "hdfs://node-master:9000/user/hadoop/covid19-country-lookup/country-lookup.csv"

const val CASES_HDFS_PATH = "hdfs://node-master:9000/user/hadoop/covid19-cases/%s.csv"

const val KAFKA_TOPIC = "processed-tweets"

// Offset format in JSON: Topic, Partition 0 and Unix time in milliseconds
const val OFFSET = """ {"%s":{"0": %s}} """

fun main() {

    withSpark(builder = SparkSession.builder()
        .enableHiveSupport()
        .appName("Covid Processor")
        .config(SparkConf()
        .set("spark.sql.shuffle.partitions", "5")
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict"))) {

        // Gets yesterday and the day before that dates
        val date = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("MM-dd-yyyy"))
        val oldDate = LocalDate.now().minusDays(2).format(DateTimeFormatter.ofPattern("MM-dd-yyyy"))
        //val date = "12-08-2020"

        // Read files
        val covidStream = spark.read().option("header", true)
            .csv(CASES_HDFS_PATH.format(date))
        val oldCovidStream = spark.read().option("header", true)
            .csv(CASES_HDFS_PATH.format(oldDate))
        val mapper = spark.read().option("header", true).csv(MAPPER_HDFS_PATH)

        // Creating start and end offset from date to milliseconds in Unix time
        val sdf = SimpleDateFormat("MM-dd-yyyy HH:mm")
        val startOffsetTime = sdf.parse("%s 00:00".format(date)).time
        val endingOffsetTime = sdf.parse("%s 23:59".format(date)).time

        // Read tweets from Kafka with start and end offset to yesterday from 00:00 to 23:59
        var tweets = spark
            .read()
            .format("kafka")
            .option("kafka.bootstrap.servers", "node-master:9092,node1:9092,node2:9092")
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsetsByTimestamp", OFFSET.format(KAFKA_TOPIC, startOffsetTime))
            .option("endingOffsetsByTimestamp", OFFSET.format(KAFKA_TOPIC, endingOffsetTime))
            .load()

        // Selects the key and value from the Kafka events
        tweets = tweets.selectExpr("CAST(key as STRING)", "CAST(value as STRING)")

        // Groups the tweets by country code and make them uppercase, maps the tweets and reduces them to country code as key and count of tweets as value
        val x =
            tweets.groupByKey { row -> row.getString(0).toUpperCase() }.mapValues { row -> row.getString(1).toLong() }
                .reduceGroups(func = { x, y -> x + y })
        x.withColumnRenamed("first", "code").withColumnRenamed("second", "tweets").createOrReplaceTempView("tweets")

        // Create tables to be able to use SQL
        covidStream.createOrReplaceTempView("covid")
        oldCovidStream.createOrReplaceTempView("old_covid")

        // Combine the two tables and count total confirmed and difference between yesterday and today
        val covid =
            spark.sql("SELECT covid.Country_Region AS country, SUM(CAST(covid.Confirmed AS BIGINT)) AS cases, SUM(CAST(covid.Confirmed AS BIGINT) - CAST(old_covid.Confirmed AS BIGINT)) AS new_cases FROM covid INNER JOIN old_covid ON covid.Combined_Key = old_covid.Combined_Key GROUP BY covid.Country_Region")

        // Update the SQL view
        covid.createOrReplaceTempView("covid")

        // Filter mapper csv so that only one row per country and create a SQL view
        mapper.filter { row -> row.isNullAt(6) }.createOrReplaceTempView("mapper")

        // Replace country name with iso2 code, adds population and tweets per country
        val newResult =
            spark.sql("SELECT iso2 as code, code3, covid.country, cases, new_cases, CAST(population AS BIGINT) AS population, CAST(COALESCE(tweets, 0) AS BIGINT) AS tweets, CAST(COALESCE(new_cases / tweets, 0) AS NUMERIC(36,2)) AS new_cases_per_tweet FROM covid INNER JOIN mapper ON covid.country = mapper.Country_Region LEFT JOIN tweets ON iso2 = tweets.code")
                .filter { row -> !row.isNullAt(0)}

        // Removes rows without a country code and creates table to be able to use SQL
        newResult.createOrReplaceTempView("result")

        // Creating Global rows with all the cases and tweets combined
        val global =
            spark.sql("SELECT 'glo' AS code, '-1' AS code3, 'Global' AS country, cases, new_cases, population, tweets, CAST(COALESCE(new_cases / tweets, 0) AS NUMERIC(36,2)) AS new_cases_per_tweet FROM ( SELECT CAST(SUM(cases) AS BIGINT) as cases, CAST(SUM(new_cases) AS BIGINT) AS new_cases, SUM(population) AS population, SUM(tweets) AS tweets FROM result )")

        // Combines the country based data with the total global data
        val complete = global.union(newResult)

        // Write the result
        complete
            .withColumn("day", lit(date))
            .write()
            .mode("append")
            .insertInto("processed")
    }
}
