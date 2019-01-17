import java.util.Properties

import org.apache.spark.sql.SparkSession

object Test {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName(Test.getClass.getSimpleName).getOrCreate()
    val oracleurl = "jdbc:oracle:thin:@172.16.230.11:1521:zdxdb"

    val oracleprop = new Properties()
    oracleprop.setProperty("user", "frontbank")
    oracleprop.setProperty("password", "sdff23s")

    val org_log_table="FRONTBANK.LOAN_BASE_LOG"

    val test_log = spark.read.jdbc(oracleurl,org_log_table,oracleprop)

    val id = test_log.repartition(2).filter(test_log("id") > 2)
    val arry = id.select("table_id").where("opertation_type=1").rdd.map(x =>x(0).toString.toInt).collect()

   // first_test.where(first_test("id").isin(arry:_*)).show()

    spark.close()





  }

}
