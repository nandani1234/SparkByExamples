package org.example.rdd.textfilereader

import org.apache.spark.sql.SparkSession

object rddTextFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").appName("TextFile").getOrCreate()

    val rdd = spark.sparkContext.textFile("/Users/nanshaks/IdeaProjects/SparkByExamples/files/*")
    rdd.foreach(f=> {
      println
    })



  }

}
