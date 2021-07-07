package com.github.kevinwallimann

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.spark.SparkException
import org.apache.spark.sql.avro.IncompatibleSchemaException
import org.apache.spark.sql.avro.SchemaConverters.toAvroType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper, the}
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.{AbrisMockSchemaRegistryClient, SchemaSubject}
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.commons.spark.SparkTestBase

class MySparkTest extends AnyFlatSpec with SparkTestBase with BeforeAndAfter {
  behavior of "Spark"
  private var mockSchemaRegistryClient: MockSchemaRegistryClient = _
  private val schemaRegistryURL = "http://localhost:8081"
  private val schemaRegistryConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistryURL)
  before {
    mockSchemaRegistryClient = new AbrisMockSchemaRegistryClient()
    SchemaManagerFactory.resetSRClientInstance()
    SchemaManagerFactory.addSRClientInstance(schemaRegistryConfig, mockSchemaRegistryClient)
  }

  it should "fail due to a nullability mismatch at runtime" in {
    val exception = the [StreamingQueryException] thrownBy executeTest(nullSafe = false)
    exception.getCause shouldBe a [SparkException]
    exception.getCause.getCause shouldBe a [IncompatibleSchemaException]
  }

  it should "succeed when using a eqNullSafe" in {
    executeTest(nullSafe = true)
  }

  private def executeTest(nullSafe: Boolean) = {
    val spark = SparkSession.builder().getOrCreate()

    val schemaCatalyst = new StructType()
      .add("value1", LongType, nullable = true)
      .add("value2", IntegerType, nullable = true)
    val value1s = (1 to 100).map(_ => 42L)
    val value2s = (1 to 100).map(_ % 5)
    val rows = value1s.zip(value2s).map(a => Row(a._1, a._2))
    val input = new MemoryStream[Row](1, spark.sqlContext)(RowEncoder(schemaCatalyst))
    input.addData(rows)

    val inputDf = input.toDF()
    val df = if(nullSafe) {
      inputDf.filter(col("value1") eqNullSafe lit(42))
    } else {
      inputDf.filter(col("value1") === lit(42)) // causes IncompatibleSchemaException later on
    }
    df.printSchema

    val allColumns = struct(df.columns.map(c => df(c)): _*)
    val avroSchema = toAvroType(allColumns.expr.dataType, allColumns.expr.nullable)
    println(avroSchema.toString(true))
    val schemaManager = SchemaManagerFactory.create(schemaRegistryConfig)
    val topic = "test-topic"
    val subject = SchemaSubject.usingTopicNameStrategy(topic, isKey = false)
    val schemaId = schemaManager.register(subject, avroSchema)
    val toAvroConfig = AbrisConfig
      .toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(schemaRegistryConfig)
    import za.co.absa.abris.avro.functions.to_avro
    val avroFrame = df.select(to_avro(allColumns, toAvroConfig) as 'value)

    val query = avroFrame
      .writeStream
      .trigger(Trigger.Once)
      .queryName("dummyQuery")
      .format("memory")
      .start()
    query.awaitTermination()
  }

}