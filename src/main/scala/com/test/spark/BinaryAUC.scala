package com.test.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._
import scala.collection.Iterator
import Array._

class BinaryAUC extends Serializable {
  //input format: predictioin,label
  def auc( data: RDD[ (Double, Double) ] ) : Double =
  {
    //group same score result
    val group_result = data.groupByKey().map(x => {
      var r = new Array[Double](2)
      for(item <- x._2) {
        if(item > 0.0) r(1) += 1.0
        else r(0) += 1.0
      }
      (x._1, r) // score, [ FalseN, PositiveN ]
    })

    //points 需要累积
    val group_rank = group_result.sortByKey(false) //big first
  //计算累积
  var step_sizes = group_rank.mapPartitions( x =>
  {
    var r = List[(Double, Double)]()
    var fn_sum = 0.0
    var pn_sum = 0.0
    while( x.hasNext )
    {
      val cur = x.next
      fn_sum += cur._2(0)
      pn_sum += cur._2(1)
    }
    r.::(fn_sum, pn_sum).toIterator
  } ,true).collect
    var debug_string = ""
    var step_sizes_sum = ofDim[Double](step_sizes.size, 2) //二维数组
    for( i <- 0 to (step_sizes.size - 1) ) {
      if(i == 0) {
        step_sizes_sum(i)(0) = 0.0
        step_sizes_sum(i)(1) = 0.0
      } else {
        step_sizes_sum(i)(0) = step_sizes_sum(i - 1)(0) + step_sizes(i - 1)._1
        step_sizes_sum(i)(1) = step_sizes_sum(i - 1)(1) + step_sizes(i - 1)._2
      }
      debug_string += "\t" + step_sizes_sum(i)(0).toString + "\t" + step_sizes_sum(i)(1).toString
    }
    val sss_len = step_sizes_sum.size
    val total_fn = step_sizes_sum(sss_len - 1)(0) + step_sizes(sss_len - 1)._1
    val total_pn = step_sizes_sum(sss_len - 1)(1) + step_sizes(sss_len - 1)._2
    //System.out.println( "debug auc_step_size: " + debug_string)

    val bc_step_sizes_sum = data.context.broadcast(step_sizes_sum)
    val modified_group_rank = group_rank.mapPartitionsWithIndex( (index, x) =>
    {
      var sss = bc_step_sizes_sum.value
      var r = List[(Double, Array[Double])]()
      //var r = List[(Double, String)]()
      var fn = sss(index)(0) //start point
    var pn = sss(index)(1)
      while( x.hasNext )
      {
        var p = new Array[Double](2)
        val cur = x.next
        p(0) = fn + cur._2(0)
        p(1) = pn + cur._2(1)
        fn += cur._2(0)
        pn += cur._2(1)
        //r.::= (cur._1, p(0).toString() + "\t" + p(1).toString())
        r.::= (cur._1, p)
      }
      r.reverse.toIterator
    } ,true)

    //output debug info
    //modified_group_rank.map(l => l._1.toString + "\t" + l._2(0).toString + "\t" + l._2(1)).saveAsTextFile("/home/hdp_teu_dia/resultdata/wangben/debug_info")

    val score = modified_group_rank.sliding(2).aggregate(0.0)(
      seqOp = (auc: Double, points: Array[ (Double, Array[Double]) ]) => auc + TrapezoidArea(points),
      combOp = _ + _
    )
    System.out.println( "debug auc_mid: " + score
      + "\t" + (total_fn*total_pn).toString()
      + "\t" + total_fn.toString()
      + "\t" + total_pn.toString() )

    score/(total_fn*total_pn)
  }

  private def TrapezoidArea(points :Array[(Double, Array[Double])]):Double = {
    val x1 = points(0)._2(0)
    val y1 = points(0)._2(1)
    val x2 = points(1)._2(0)
    val y2 = points(1)._2(1)

    val base = x2 - x1
    val height = (y1 + y2)/2.0
    return base*height
  }
}