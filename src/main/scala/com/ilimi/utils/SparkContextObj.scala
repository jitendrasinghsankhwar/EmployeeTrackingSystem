package com.ilimi.utils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkContextObj {
  def getSparkContext: SparkContext = {
    val conf = new SparkConf().setAppName("EmployeeTrackingSystem").setMaster("local").set("spark.cassandra.connection.host", "127.0.0.1")
    return new SparkContext(conf)
  }
}