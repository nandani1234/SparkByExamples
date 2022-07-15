package org.example.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object rddParallelize {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName("RddExamples").getOrCreate()
    val rdd1: RDD[Int] = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))

    val emptyRdd = spark.sparkContext.parallelize(Seq.empty[String])
    val emptyRdd1 = spark.sparkContext.parallelize(List())

    emptyRdd.foreach(println)

    println(Seq.empty[String])

    val rddCollect: Array[Int] = rdd1.collect()

    println("number of partitions",rdd1.getNumPartitions)
    println("Actions",rdd1.first())
    rddCollect.foreach(println)


  }

}
