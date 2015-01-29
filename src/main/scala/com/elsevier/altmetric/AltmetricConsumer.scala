package com.elsevier.altmetric

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


object AltmetricConsumer {

  val conf = new SparkConf().setMaster("local[2]").setAppName("kafka-consumer")
  val ssc =   new StreamingContext(conf,Seconds(1))


  val lines = ssc.socketTextStream("localhost",9999)

   // split each line into words
  val words = lines.flatMap(_.split(","))

 val pairs = words.map(word => (word,1))

  val wordCount = pairs.reduceByKey(_+_)


   wordCount.print()



}
