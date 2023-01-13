import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * The aim of this class is to encapsulate the logic responsible for reading/writing the data of a given csv file
 */
object DataTransformer {
  val sparkHost : String = "local"
  val hdfsRawPath : String = "hdfs://192.168.1.2:9000/raw/locaux"
  val hdfsBronzePath : String = "hdfs://192.168.1.2:9000/bronze/locaux"
  val hdfsSilverPath : String = "hdfs://192.168.1.2:9000/silver/locaux"

  // Spark Session
  val sparkSession: SparkSession = SparkSession
    .builder
    .master(sparkHost)
    .appName("pds-etl-manager")
    .enableHiveSupport()
    .getOrCreate()

  def transformDataBronze(): Try[DataFrame] = {
    // Log
    println(s"\n${"-" * 25} READING FILE STARTED ${"-" * 25}")
    println(s"reading from ${hdfsRawPath}")

    // Spark-session context
    sparkSession.sparkContext.setCheckpointDir("tmp")
    sparkSession.sparkContext.setLogLevel("ERROR")

    // Load data from HDFS
    try{
      // First Iteration
      val df = sparkSession.read.parquet(hdfsRawPath)

      val df_bronze = df
        // Delete unuseful columns
        .drop("RatecodeID", "store_and_fwd_flag", "PULocationID",
          "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
          "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge",
          "airport_fee")
        // Rename columns names
        .withColumnRenamed("VendorID" , "RoomId")
        .withColumnRenamed("tpep_pickup_datetime" , "start_time")
        .withColumnRenamed("tpep_dropoff_datetime" , "end_time")
        .withColumnRenamed("passenger_count" , "nb_persons")
        .withColumnRenamed("trip_distance" , "mult_factor")

      df_bronze.show(20)

      val hasWritten = writeParquetToHDFS(hdfsBronzePath, df_bronze)

      if (!hasWritten) Failure(new Throwable("cannot save bronze data file"))
      println("Bronze Data File Has Been Successfully Written")

      Success(df_bronze)
    } catch {
      case e: Throwable =>
        Failure(new Throwable(s"cannot read/save file: ${e.getMessage}"))
    }
  }

  def transformDataSilver(): Boolean = {
    // Log
    println(s"\n${"-" * 25} READING FILE STARTED ${"-" * 25}")
    println(s"reading from ${hdfsBronzePath}")

    // Spark-session context
    sparkSession.sparkContext.setCheckpointDir("tmp")
    sparkSession.sparkContext.setLogLevel("ERROR")

    // Load data from HDFS
    try {
      // Second Iteration
      val df = sparkSession.read
        .schema(Helper.sparkSchemeFromJSON())
        .option("mode", "DROPMALFORMED")
        .parquet(hdfsBronzePath)

      // prepare
      val df_str = df
        .withColumn("start_time", col("start_time").cast("String"))
        .withColumn("end_time", col("end_time").cast("String"))

      // clean data
      import sparkSession.implicits._
      val ds = df_str.map(row => {
        val roomId = row.getLong(0)

        val start_date = transformTime(row.getString(1))

        var end_date = row.getString(2)

        if (start_date != row.getString(1)) {
          end_date = transformTime(end_date)
        }
        val nb_persons = row.getDouble(3) * row.getDouble(4)

        (roomId, start_date, end_date, nb_persons.toInt + 3)
      })

      val df_cleaned = ds.toDF("RoomId", "start_date", "end_date", "nb_persons")

      df_cleaned.show(100)

      val df_silver = df_cleaned.filter(df_cleaned("nb_persons").isNull)

      // Write to final
      val hasWritten = writeParquetToHDFS(hdfsSilverPath, df_silver)

      if (!hasWritten) Failure(new Throwable("cannot save silver data file"))
      println("Silver Data File Has Been Successfully Written")
      true
    }
    catch {
      case e: Throwable =>
        println(s"error while saving silver data: ${e.getMessage}")
        false
    }
  }

  def transformTime(datetime: String): String = {
    val date_time = datetime.split(' ')
    // Dates
    val date = date_time(0)
    val time = date_time(1)

    // Time
    val time_arr = time.split(':')
    var hour = time_arr(0).toLong
    val minutes = time_arr(1).toLong
    val seconds = time_arr(2)

    // Transformation
    if (hour < 7) {
      hour = hour + 12
    } else if (hour > 19) {
      hour = hour - 12
    }

    s"${date} ${hour}:${minutes}:${seconds}"

  }

  def writeParquetToHDFS(hdfsPath: String, df : DataFrame): Boolean = {
    println(s"${"-" * 25} SAVING FILE STARTED ${"-" * 25}")

    sparkSession.sparkContext.setLogLevel("ERROR")

    try {
      // Write to final
      df.checkpoint(true)
        .write
        .mode(SaveMode.Append)
        .save(hdfsPath)
      true
    }
    catch {
      case e: Throwable =>
        println(s"error while saving data: ${e.getMessage}")
        false
    }
  }
}
