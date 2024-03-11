package scalatest;
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}
import java.util.concurrent.TimeUnit
import scala.util.Random


object Kafka_Load extends App {

  // Parameters for the script
  val url   = "http://18.133.73.36:5001/insurance_claims1" // API URL to fetch the JSON data
  val topic = "insurance_claims_5-3-12-99" // Kafka topic name & table name in HBase (or Hive)

  // Initialize Spark session for DataFrame and Dataset APIs
  val spark = SparkSession.builder.appName("API to Kafka _ 2").getOrCreate()
  import spark.implicits._

  // Function to fetch data from API
  def fetchDataFromAPI(apiUrl: String): String = {
    Source.fromURL(apiUrl).mkString
  }

  // Fetch data from URL and convert it to a string
  val result = Source.fromURL(url).mkString

  // Read the JSON data into a DataFrame
  val jsonData = spark.read.json(Seq(result).toDS)
  jsonData.show() // Display the DataFrame contents

  // Kafka servers configuration
  val kafkaServers = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"

  // Produce data every 5 seconds
  while (true) {
    val data = fetchDataFromAPI(url)
    spark.read.json(Seq(data).toDS)
      .selectExpr("to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", topic)
      .save()
    println(s"Published data to Kafka at ${java.time.LocalDateTime.now()}")
    TimeUnit.SECONDS.sleep(5)
  }
  
  // Consume data and load into HBase every 10 seconds
  while (true) {
      val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      println(s"Loaded data from Kafka to HBase at ${java.time.LocalDateTime.now()}")
      import spark.implicits._
      val messages = df.selectExpr("CAST(value AS STRING) as message")
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "zookeeper-server1,zookeeper-server2,zookeeper-server3")
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      hbaseConf.set("zookeeper.znode.parent", "/hbase")
      val connection = ConnectionFactory.createConnection(hbaseConf)
      val tableName = TableName.valueOf(topic)
      val columnFamilyName = "cf"
      val admin = connection.getAdmin
      if (!admin.tableExists(tableName)) {
        val tableDescriptor = new HTableDescriptor(tableName)
        val columnDescriptor = new HColumnDescriptor(columnFamilyName)
        tableDescriptor.addFamily(columnDescriptor)
        admin.createTable(tableDescriptor)
        println(s"Table $tableName created.")
      }
      val table = connection.getTable(tableName)
      def generateUniqueRowKey(): String = {
        val currentTime = System.currentTimeMillis()
        val random = new Random()
        s"$currentTime-${random.nextInt(10000)}"
      }
      messages.collect().take(1000).foreach { message =>
        val rowKey = generateUniqueRowKey()
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("data"), Bytes.toBytes(message.getString(0)))
        table.put(put)
      }
      println("Finished loading data to HBase.")
      table.close()
      connection.close()
      TimeUnit.SECONDS.sleep(10) // Sleep for 10 seconds
  }

  // Stop Spark session
  spark.stop()
}
