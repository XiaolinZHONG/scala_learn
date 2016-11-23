import org.apache.spark.sql.DataFrame
import Array._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhongxl on 2016/11/19.
  */

object BPNeturalNetworks {

  var sizes: Array[Int] = null
  // 网络结构
  //  var numLayers: Int = sizes.length
  // 网络层次

  var biases :ArrayBuffer[Array[Array[Double]]] = null
  // bias 动态录入


  var weights :ArrayBuffer[Array[Array[Double]]]=null
  // weights 动态录入


  def initNetworks()={

    for (i <- 1 until sizes.length) {
      var tempMatrix = ofDim[Double](sizes(i), 1)
      for (j <- 0 until sizes(i)) {
        tempMatrix(j)(0) = math.random;
      }
      biases += tempMatrix;
    }

    for (x <- 1 until sizes.length; y <- 0 until sizes.length - 1) {
      var tempMatrix = ofDim[Double](sizes(x), sizes(y))
      for (i <- 0 until sizes(x); j <- 0 until sizes(y)) {
        tempMatrix(i)(j) = math.random;
      }
      weights += tempMatrix
    }

  }
  def feedForward(z:DataFrame)={
    for (b<-biases;w<-weights){

    }
  }

  def main(args: Array[String]) {

    sizes = Array(3, 3, 2, 1)
    val epochs =10
    val miniBatchSize=10
    val eta=0.1

  }

}
