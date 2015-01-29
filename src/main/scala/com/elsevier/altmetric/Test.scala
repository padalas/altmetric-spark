package com.elsevier.altmetric

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by PADALAS on 27/01/2015.
 */

object Test {

  def main(args: Array[String]) {
    val logFile = "C:\\IMGVERWIN70214.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}
