


object sparkhive {

  def main(args: Array[String]): Unit = {
    import java.io.File

    import org.apache.spark.sql.{Row, SaveMode, SparkSession}

    case class Record(key: Int, value: String)

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH '/usr/local/Cellar/spark-2.3.0/examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    //    sql("SELECT * FROM src").show()

    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    sqlDF.show()

    spark.close()
  }

}