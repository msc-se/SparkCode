package com.sdu.big.data

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.Optional
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import scala.Tuple2
import java.io.UnsupportedEncodingException
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.collections.HashMap
import kotlin.text.Charsets.UTF_8
import org.apache.spark.api.java.function.Function2
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import java.io.Serializable
import java.time.*

data class Place(val country_code: String)

data class Tweet(val text: String, val place: Place?)

data class State(val date: Long, val count: Int) : Serializable

data class Word(val word: String, val count: Int)

val gson = Gson()

class TweetDeserializer : Deserializer<Tweet> {

    override fun deserialize(topic: String, data: ByteArray): Tweet {
        return try {
            gson.fromJson(String(data, UTF_8), Tweet::class.java)
        } catch (var4: UnsupportedEncodingException) {
            throw SerializationException("Error when deserializing byte[] to string due to unsupported encoding ")
        }
    }

}


var updateFunction = Function2<List<Int>, Optional<State>, Optional<State>> { values, state ->
    var newSum = if (!state.isPresent || state.get().date != LocalDate.now(ZoneOffset.UTC).toEpochDay()) {
        0
    } else {
        state.get().count
    }
    for (i in values) {
        newSum += i
    }
    if (newSum > 0) {
        Optional.of(State(LocalDate.now(ZoneOffset.UTC).toEpochDay(), newSum))
    } else {
        Optional.absent()
    }
}

// Kafka offset checkpoint path
const val checkpoint = "hdfs:///user/hadoop/tweets-offset"

const val nodes = "node-master:9092,node1:9092,node2:9092"

// Kafka consumer properties
val kafkaParams: HashMap<String, Any> = hashMapOf(
    "bootstrap.servers" to nodes,
    "key.deserializer" to StringDeserializer::class.java.name,
    "value.deserializer" to TweetDeserializer::class.java.name,
    "group.id" to "spark"
)

val topics = listOf("tweets")

fun main() {

    // Kafka Producer properties
    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = nodes
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

    // Creates the streaming context with all the processing and functionality
    val javaStreaming = JavaStreamingContext.getOrCreate(checkpoint) {
        val conf = SparkConf().setAppName("Kafka processor").set("spark.sql.shuffle.partitions", "5")
            .set("hive.exec.dynamic.partition", "true").set("hive.exec.dynamic.partition.mode", "nonstrict")
        val jssc = JavaStreamingContext(conf, Durations.seconds(60))

        // Creates Direct stream
        val stream: JavaInputDStream<ConsumerRecord<String, Tweet>> =
            KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
            )

        val re = Regex("\\w+")

        val wordCount =
            stream.flatMap { tweet -> re.findAll(tweet.value().text.toLowerCase()).map { it.value }.iterator() }
                .mapToPair { tweet -> Tuple2(tweet, 1) }

        val wordCountResult = wordCount.updateStateByKey(updateFunction).mapValues { it.count }

        wordCountResult.foreachRDD { rdd ->
            val spark = SparkSession.builder().appName("Spark Session").enableHiveSupport()
                .config(conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")).orCreate
            val date = LocalDate.now(ZoneOffset.UTC).format(
                DateTimeFormatter.ofPattern("MM-dd-yyyy")
            )
            val word = rdd.map { Word(it._1, it._2) }
            val df = spark.createDataFrame(word, Word::class.java).withColumn("day", lit(date))

            df.write()
                .mode(SaveMode.Overwrite)
                .insertInto("wordcount")

        }

        // Creates a key value pair with Global as key and the total number of tweets
        val global = stream.count().mapToPair { c -> Tuple2("Global", c) }

        // Removes every tweet with no location data and maps the different tweets by their country code. Lastly counts the tweets in each country code
        val countriesCount = stream.filter { record -> record.value().place != null }
            .map { record -> record.value().place!!.country_code }.countByValue()

        // Combines country based tweet counts and global tweet count
        val result = countriesCount.union(global)


        // Saves each RDD from the DStream to Kafka both the global and country based counts
        // But only saves the country based counts if they are above 0
        result.foreachRDD { rdd ->
            rdd.foreachPartition { iter ->
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
            }
        }

        // Sets the Kafka checkpoint in HDFS
        jssc.checkpoint(checkpoint)
        jssc
    }
    // Starts the streaming and awaits the termination
    javaStreaming.start()
    javaStreaming.awaitTermination()
}
