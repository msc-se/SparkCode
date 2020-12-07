package com.sdu.big.data

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
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import scala.Tuple2
import java.io.UnsupportedEncodingException
import java.util.*
import kotlin.collections.HashMap
import kotlin.text.Charsets.UTF_8

data class Place(val countryCode: String, val  country: String)

data class Tweet(val place: Place?)

class TweetDeserializer: Deserializer<Tweet> {

    override fun deserialize(topic: String, data: ByteArray): Tweet {
        return try {
            Gson().fromJson(String(data, UTF_8), Tweet::class.java)
        } catch (var4: UnsupportedEncodingException) {
            throw SerializationException("Error when deserializing byte[] to string due to unsupported encoding ")
        }
    }

}


const val checkpoint = "hdfs://node-master:9000/user/hadoop/tweets-offset"

val kafkaParams: HashMap<String, Any> = hashMapOf(
    "bootstrap.servers" to "node-master:9092",
    "key.deserializer" to StringDeserializer::class.java.name,
    "value.deserializer" to TweetDeserializer::class.java.name,
    "group.id" to "spark"
)

val topics = listOf("tweets")


fun main() {

    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "node-master:9092"
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java


    val javaStreaming = JavaStreamingContext.getOrCreate(checkpoint) {
        val conf = SparkConf().setAppName("Kafka processor").set("spark.sql.shuffle.partitions", "5")
        val jssc = JavaStreamingContext(conf, Durations.seconds(10))

        val stream: JavaInputDStream<ConsumerRecord<String, Tweet>> =
            KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams))

        val global = stream.count().mapToPair { c -> Tuple2("Global", c) }
        val countries = stream.filter { record -> record.value().place != null }.map { record -> record.value().place!!.countryCode }
        val countriesCount = countries.countByValue()


        val result = countriesCount.union(global)


        result.foreachRDD { rdd -> rdd.foreachPartition { iter ->
            val producer = KafkaProducer<String, String>(properties)
            while (iter.hasNext()) {
                val next = iter.next()
                if (next._2 > 0) {
                    val record = ProducerRecord("processed-tweets", next._1.toLowerCase(), next._2.toString())
                    producer.send(record)
                }
            }
            producer.flush()
            producer.close()
        } }

        jssc.checkpoint(checkpoint)
        jssc
    }

    javaStreaming.start()
    javaStreaming.awaitTermination()

}
