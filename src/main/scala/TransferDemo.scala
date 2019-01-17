import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object TransferDemo {


  def main(args: Array[String]): Unit = {


    val url = "jdbc:mysql://172.16.250.9:3306/pms?characterEncoding=utf-8"
    val url2 = "jdbc:mysql://10.100.200.18:3306/report?characterEncoding=utf-8"

    val tableName = "pms_organization_info"
    val columnName = "id"
    val lowerBound = 1
    val upperBound = 1200
    val numPartitions = 1
    //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0")

    println("表的最大id"+ upperBound)

    // val spark = SparkSession.builder().master("local").appName(TransferDatas.getClass.getSimpleName).getOrCreate()
    val spark = SparkSession.builder().master("local[*]").appName(TransferDemo.getClass.getSimpleName).getOrCreate()

    // 设置连接用户&密码
    val prop = new Properties()
    prop.setProperty("user","u_uat_read")
    prop.setProperty("password","password")

    val prop2 = new Properties()
    prop2.setProperty("user", "u_report")
    prop2.setProperty("password", "report123456")
    // 取得该表数据
    val jdbcDF = spark.read.jdbc(url,tableName, columnName, lowerBound, upperBound,numPartitions,prop)

    // val org_data= jdbcDF.select("loan_id","residual_pact_money","penalty_rate","rateed")

        //jdbcDF.write.format("com.databricks.spark.csv").save("newcars")
    //jdbcDF.repartition(2).write.mode(SaveMode.Append).jdbc(url2, "pms_organization_info", prop2)
    Thread.sleep(30000)


    spark.close()




  }

}
