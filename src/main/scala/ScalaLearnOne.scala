import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhongxl on 2016/11/17.
  */

object ScalaLearnOne{
  def main(args: Array[String]) {

    // 声明

    val spark=SparkSession.builder().master("local")
      .appName("spark_mllib")
      .config("spark.sql.warehouse.dir","file:///")
      .getOrCreate()

    val sqlContext = spark


    val adress="D:/project_csm/bigtable_2B_xc_new.csv"
    val df= sqlContext.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load(adress)
    df.show()
  }
}