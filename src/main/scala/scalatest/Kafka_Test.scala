package scalatest;
import org.apache.spark.sql.{DataFrame, SparkSession}

object Kafka_Test {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("TestAPIReadingAndKafkaCreation")
      .master("local[2]")
      .getOrCreate()

    try {
      // Read API response into DataFrame
      val apiResponseDF = spark.read.json("http://18.133.73.36:5001/insurance_claims1")

      // Create Kafka topic
      apiResponseDF.write.mode("append").saveAsTable("kafka_topic")

      // Verify that the API was read and Kafka topic was created
      assert(apiResponseDF.count() > 0)
      assert(spark.catalog.tableExists("kafka_topic"))
    } catch {
      case e: Exception =>
        println(s"Test failed: ${e.getMessage}")
    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }
}
