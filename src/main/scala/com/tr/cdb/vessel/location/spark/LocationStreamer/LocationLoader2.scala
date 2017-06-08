package com.tr.cdb.vessel.location.spark.LocationStreamer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd.RDD


//spark-submit --class com.tr.cdb.vessel.location.spark.LocationStreamer.LocationLoader2 --master yarn-cluster --deploy-mode cluster LocationStreamer-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
object LocationLoader2 {
  def main(args: Array[String]) {

    def ReceivedParameters = args.mkString("<", ",", ">")
    System.out.println(ReceivedParameters)

    val conf = new SparkConf().setAppName("LocationLoader")

    val ssc = new StreamingContext(conf, Seconds(10))

    val topics = List(("vessel_location", 4)).toMap

    val topicLines = KafkaUtils.createStream(ssc, "dsj-s6:2181,dsj-s3:2181,dsj-s1:2181/kafka", "stream_group", topics)

    topicLines.foreachRDD { x => {
      
       x.foreach { println(_)};
      
    } }
    


    ssc.start()

    ssc.awaitTermination()

  }
}