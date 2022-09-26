package com.github.supermariolabs.spooq.udf.avro

import com.github.supermariolabs.spooq.udf.SimpleUDF
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{Decoder, DecoderFactory}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions
import java.util

class AvroDecoder extends SimpleUDF {
    private val HEADER: Array[Byte] = Array[Byte](0xc3.toByte, 0x01.toByte)
    private val FINGERPRINT_SIZE = 8
    private val HEADER_LENGTH = HEADER.length + FINGERPRINT_SIZE

    def decode(data: Array[Byte], schemaStr: String): String = {
        var payload = data
        if (isAvroSingleObjectEncoded(data)) payload = data.slice(HEADER_LENGTH,data.size)
        val schema = new Schema.Parser().parse(schemaStr)

        val decoderFactory: DecoderFactory = new DecoderFactory
        val decoder: Decoder = decoderFactory.binaryDecoder(payload,null)

        val datumReader = new GenericDatumReader[GenericRecord](schema)
        val record: GenericRecord = datumReader.read(null, decoder)

        record.toString
    }

    def isAvroSingleObjectEncoded(rawData: Array[Byte]): Boolean = {
        util.Arrays.equals(rawData.slice(0,HEADER.size), HEADER)
    }

    override val udf: UserDefinedFunction = functions.udf(decode(_:Array[Byte],_:String))
}
