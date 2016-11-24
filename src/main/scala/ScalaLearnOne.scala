import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by zhongxl on 2016/11/17.
  */

object ScalaLearnOne {
  def main(args: Array[String]) {

    // 声明

    val spark = SparkSession.builder.master("local")
      .appName("spark_ml").config("spark.sql.warehouse.dir", "file:///")
      .getOrCreate()




    val adress = "D:/project_csm/bigtable_2B_xc_new.csv"
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(adress)
    df.show(3)


    def kMeansDiscreteData(oriData: DataFrame, flag: String): DataFrame = {

      val args = oriData.select(flag).columns
      val vectorAssembler = new VectorAssembler()
        .setInputCols(args)
        .setOutputCol("features")
      val kMeans = new KMeans().setK(50).setFeaturesCol("features").setPredictionCol(flag + "km")
      val pipeline = new Pipeline().setStages(Array(vectorAssembler, kMeans))
      case class Temp(flag: Double)
      val model = pipeline.fit(oriData) // 忽略这个告警
      val data = model.transform(oriData).drop("features").drop(flag) //忽略此告警
      return data
    }

    val data = kMeansDiscreteData(df, "uid_grade")
    data.show()
    val s = data.stat.approxQuantile("uid_age", Array(0.25, 0.5, 0.75), 0.2)
    println(s.toList)
    val q1 = s(0)
    val q3 = s(2)
    val iqr = q3 - q1
    val lowerRange = q1 - 1.5 * iqr
    val upperRange = q3 + 1.5 * iqr
    println(iqr, lowerRange, upperRange)
    data.filter("uid_age>15 and uid_age<45").show(10)
    val outlier = data.filter(s"uid_age>$lowerRange and uid_age<$upperRange")
    outlier.show(5)

    val age = data.where(s"uid_age<$lowerRange or uid_age>$upperRange").select("uid_age")
    age.withColumn("uid_age", age("uid_age") * 0 + 20).show()





    def outlierDataProcess(dataFrame: DataFrame): DataFrame = {
      var newdata = dataFrame
      for (flag <- dataFrame.columns.toList) {
        val quantiles = dataFrame.select(flag).stat.approxQuantile(flag, Array(0.25, 0.5, 0.75), 0.1)
        val q1 = quantiles(0)
        val median = quantiles(1)
        val q3 = quantiles(2)
        val iqr = q3 - q1
        val lowerRange = q1 - 1.5 * iqr
        val upperRange = q3 + 1.5 * iqr
        //        println("FOLLOWS ARE THE OUTLIER DATA OF " + flag)
        //        dataFrame.select(flag).filter(s"$flag<$lowerRange or $flag > $upperRange").show(15)
        val compare = udf { (row: Int) =>
          if (row > upperRange || row < lowerRange) {
            Math.floor(median) // 向下取整数
          }
          else {
            row //注意这里不能写成return
          }
        }
        val juge = udf { (row: Int) =>
          if (row == -1) {
            0
          } // 业务上未填写即为-1
          else {
            1
          }
        }
        newdata = newdata.withColumn(flag + "_new", compare(col(flag)))
          .withColumn(flag + "TF", juge(col(flag))).drop(flag)
        //注意这种更新DataFrame方式
      }
      return newdata

    }
    println("-" * 50)
    outlierDataProcess(df.select("uid_age", "uid_grade")).show(5)

  }
}
