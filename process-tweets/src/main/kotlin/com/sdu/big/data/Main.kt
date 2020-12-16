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
import org.apache.spark.streaming.api.java.JavaPairDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import scala.Tuple2
import java.io.UnsupportedEncodingException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.collections.HashMap
import kotlin.text.Charsets.UTF_8
import org.apache.spark.api.java.function.Function2

data class Place(val country_code: String)

data class Tweet(val text: String, val place: Place?)

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

var updateFunction = Function2<List<Int>, Optional<Int>, Optional<Int>> { values, state ->
    var newSum: Int = state.orElse(0)

    for (i in values) {
        newSum += i
    }
    Optional.of(newSum)
}

const val WORD_COUNT_PATH = "hdfs:///user/hadoop/word-count/%s"

const val checkpoint = "hdfs:///user/hadoop/tweets-offset1"

const val nodes = "node-master:9092,node1:9092,node2:9092"

val kafkaParams: HashMap<String, Any> = hashMapOf(
    "bootstrap.servers" to nodes,
    "key.deserializer" to StringDeserializer::class.java.name,
    "value.deserializer" to TweetDeserializer::class.java.name,
    "group.id" to "spark"
)

val topics = listOf("tweets")

var today = LocalDate.now()


fun main() {

    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = nodes
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java


    val javaStreaming = JavaStreamingContext.getOrCreate(checkpoint) {
        val conf = SparkConf().setAppName("Kafka processor").set("spark.sql.shuffle.partitions", "5")
        val jssc = JavaStreamingContext(conf, Durations.seconds(60))

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

        val wordCountResult: JavaPairDStream<String, Int>

        if (today == LocalDate.now()) {
            wordCountResult = wordCount.updateStateByKey(updateFunction)
        } else {
            wordCountResult = wordCount.reduceByKey { x, y -> x + y }
            today = LocalDate.now()
        }

        val now = today.format(DateTimeFormatter.ofPattern("MM-dd-yyyy"))
        wordCountResult.foreachRDD { rdd -> rdd.saveAsTextFile(WORD_COUNT_PATH.format(now)) }


        val global = stream.count().mapToPair { c -> Tuple2("Global", c) }
        val countries = stream.filter { record -> record.value().place != null }
            .map { record -> record.value().place!!.country_code }
        val countriesCount = countries.countByValue()


        val result = countriesCount.union(global)


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

        jssc.checkpoint(checkpoint)
        jssc
    }

    javaStreaming.start()
    javaStreaming.awaitTermination()

}
