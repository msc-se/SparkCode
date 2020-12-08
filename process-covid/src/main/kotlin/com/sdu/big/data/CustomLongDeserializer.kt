package com.sdu.big.data.covid

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.errors.SerializationException
import kotlin.text.Charsets.UTF_8
import java.io.UnsupportedEncodingException

class CustomLongDeserializer: Deserializer<Long> {
    override fun deserialize(topic: String, data: ByteArray): Long {
        return try {
               String(data, UTF_8).toLong()
        } catch (var4: UnsupportedEncodingException) {
            throw SerializationException("Error when deserializing byte[] to string due to unsupported encoding ")
        }
    }
}