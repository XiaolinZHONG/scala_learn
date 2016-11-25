import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhongxl on 2016/11/17.
  */

object ScalaLearnOne {
  def main(args: Array[String]) {

    // 声明

    val spark = SparkSession.builder.master("local")
      .appName("spark_ml")
//      .config("spark.sql.warehouse.dir", "file:///")
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
      val kMeans = new KMeans().setK(50).setFeaturesCol("features").setPredictionCol(flag + "_km")
      val pipeline = new Pipeline().setStages(Array(vectorAssembler, kMeans))
      case class Temp(flag: Double)
      val model = pipeline.fit(oriData)
      val data = model.transform(oriData).drop("features").drop(flag)
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
        val quantiles = dataFrame.select(flag)
          .stat.approxQuantile(flag, Array(0.25, 0.5, 0.75), 0.1)
        val q1 = quantiles(0)
        val median = quantiles(1)
        val q3 = quantiles(2)
        val iqr = q3 - q1
        val lowerRange = q1 - 1.5 * iqr
        val upperRange = q3 + 1.5 * iqr
        println("FOLLOWS ARE THE OUTLIER DATA OF " + flag)
        dataFrame.select(flag).filter(s"$flag<$lowerRange or $flag > $upperRange").show(3)
        val compare = udf { (row: Int) =>
          if (row > upperRange || row < lowerRange) {Math.floor(median)}
          else {row}
        }
        val juge = udf { (row: Int) =>
          if (row == -1) {0}
          else {1}}
        newdata = newdata.withColumn(flag + "_new", compare(col(flag)))
          .withColumn(flag + "TF", juge(col(flag))).drop(flag)
        //注意这种更新DataFrame方式
      }
      return newdata
    }

    println("-" * 50)
    outlierDataProcess(df.select("uid_age", "uid_grade")).show(5)

    def normalDataProcess(dataFrame: DataFrame): DataFrame = {
      var newData = dataFrame
      for (flag <- dataFrame.columns.toList) {
        val describes = dataFrame.describe(flag)
        //        describes.show()
        val meanVal = describes.take(3)(1).toSeq(1).toString.toDouble
        val stdVal = describes.take(3)(2).toSeq(1).toString.toDouble
        val maxVal = meanVal + 3 * stdVal
        val minVal = meanVal - 3 * stdVal
        val compare = udf { (row: Int) =>
          if (row > maxVal || row < minVal) {Math.floor(meanVal)}
          else {row}}
        val juge = udf { (row: Int) =>
          if (row == -1) {0}
          else {1}}
        newData = newData
          .withColumn(flag + "_new", compare(col(flag))).drop(flag)
      }
      return newData
    }

    normalDataProcess(df.select("uid_age", "uid_grade")).show(5)

    def outliersProcess(dataFrame: DataFrame, replace: Int = 95): DataFrame = {
      var newData = dataFrame
      for (flag <- dataFrame.columns.toList) {
        val quantiles = dataFrame.select(flag)
          .stat.approxQuantile(flag, Array(0.5, 0.95), 0.1)
        val q95 = quantiles(1)
        val q5 = quantiles(0)
        val compare95 = udf { (row: Int) =>
          if (row > q95) {Math.floor(q95)}
          else if (row == -1) {q5}
          else {row}}
        val compare5 = udf { (row: Int) =>
          if (row > q95) {Math.floor(q5)}
          else if (row == -1) {q5}
          else {row}}

        if (replace == 5) {
          newData = newData.withColumn(flag + "_new", compare5(col(flag))).drop(flag)
        }
        else {
          newData = newData.withColumn(flag + "_new", compare95(col(flag))).drop(flag)
        }
      }
      return newData
    }

    outliersProcess(df.select("uid_age", "uid_grade")).show(5)




  }
}
