import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/*
实现将oracle数据库当中的数据导入到另外一个oracle数据库当中，通过设置表当中数据按照多少个分区进行导入操作。
这里指定最大id和最小的id然后进行划分操作，然后按照分区读取数据然后写数据
 */

object TransferDatas {

  def main(args: Array[String]): Unit = {
    val url = "jdbc:oracle:thin:@172.16.250.10:1521:stupor"
    val url2 = "jdbc:oracle:thin:@172.16.230.11:1521:zdxdb"
  //  val url2 = "jdbc:mysql://10.100.200.18:3306/report?characterEncoding=utf-8"
    val or=new Operation_record()
    val tableName = "XD_CORE.OVERDUE_DETAIL"
    val columnName = "id"
    val lowerBound = 1
    val upperBound = or.getmaxId(tableName)
    val numPartitions = 20

    println("表的最大id"+ upperBound)

    val spark = SparkSession.builder().master("local[*]").appName(TransferDatas.getClass.getSimpleName).getOrCreate()
    //val spark = SparkSession.builder().appName(TransferDatas.getClass.getSimpleName).getOrCreate()

    // 设置连接用户&密码
    val prop = new Properties()
    prop.setProperty("user","qry_read")
    prop.setProperty("password","password")

 /*
    val prop2 = new Properties()
    prop2.setProperty("user", "u_report")
    prop2.setProperty("password", "report123456")
    */
          val prop2 = new Properties()
          prop2.setProperty("user", "frontbank")
          prop2.setProperty("password", "sdff23s")
    // 取得该表数据
    val jdbcDF = spark.read.jdbc(url,tableName, columnName, lowerBound, upperBound,numPartitions,prop)

   // val org_data= jdbcDF.select("loan_id","residual_pact_money","penalty_rate","rateed")
       jdbcDF.repartition(30).write.mode(SaveMode.Append).jdbc(url2, "FRONTBANK.OVERDUE_DETAIL", prop2)


    spark.close()


  }

}
