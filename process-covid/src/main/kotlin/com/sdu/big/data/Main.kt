package com.sdu.big.data.covid

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.jetbrains.kotlinx.spark.api.*
import scala.Tuple2
import java.io.UnsupportedEncodingException
import java.util.*
import kotlin.collections.HashMap
import kotlin.text.Charsets.UTF_8


const val checkpoint = "hdfs://node-master:9000/user/hadoop/processed-tweets-offset"

val kafkaParams: HashMap<String, Any> = hashMapOf(
    "bootstrap.servers" to "node-master:9092",
    "key.deserializer" to StringDeserializer::class.java.name,
    "value.deserializer" to StringDeserializer::class.java.name,
    "group.id" to "spark"
)

val topics = listOf("processed-tweets")


fun main() {

    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "node-master:9092"
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

    withSpark(appName = "Covid Processor", props = mapOf("spark.sql.shuffle.partitions" to "5")) {
        val schema = spark.read().csv("hdfs://node-master:9000/user/hadoop/covid19-cases/header.csv").schema()
        val stream = spark.readStream().schema(schema).option("header", true)
            .csv("hdfs://node-master:9000/user/hadoop/covid19-cases/")

        val map = stream.map { row -> Tuple2(row.getString(3), if (row.isNullAt(7)) 0 else row.getLong(7)) }
            .filter { row -> row._2 != null }.groupByKey { row -> row._1 }.count()

        val query = map.writeStream()
            .outputMode("complete")
            .format("console")
            .start()

        query.awaitTermination()
    }

}
