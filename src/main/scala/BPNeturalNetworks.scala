import org.apache.spark.sql.DataFrame
import Array._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhongxl on 2016/11/19.
  */

object BPNeturalNetworks {

  var sizes: Array[Int] = null
  // 网络结构
  var numLayers: Int = sizes.length
  // 网络层次
  var epochs: Int = null
  // 循环次数
  var miniBatchSize: Int = null
  // 小批量的大小
  var eta: Int = null
  // 步长
  var biases = ArrayBuffer()


  for (i <- 1 until sizes.length) {
    var matrix = Array.ofDim[Double](sizes(i), 1)
    for (j <- 0 to sizes(i)) {
      matrix(j)(1) = math.random
    }
    biases
  }


  /**
    * 前馈函数
    *
    * @param dataFrame
    */
  def feedForward(dataFrame: DataFrame) = {

  }
}
