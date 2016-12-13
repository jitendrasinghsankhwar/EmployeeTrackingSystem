package com.ilimi.utils

import java.io.File

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.Period
import org.joda.time.format.DateTimeFormat

import com.datastax.spark.connector.toSparkContextFunctions
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Utils {
    val sc = getSparkContext;

    def getSparkContext: SparkContext = {
        val conf = new SparkConf().setAppName("EmployeeTrackingSystem").setMaster("local").set("spark.cassandra.connection.host", "127.0.0.1")
        return new SparkContext(conf)
    }

    def getAllDates(startDate: String, endDate: String): Iterator[DateTime] = {
        def dateRange(start: DateTime, end: DateTime, step: Period): Iterator[DateTime] = Iterator.iterate(start)(_.plus(step)).takeWhile(!_.isAfter(end))
        val range = dateRange(DateTime.parse((startDate + " 09:00:00"), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC()), DateTime.parse((endDate + " 09:00:00"), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC()), Period.days(1))
        return range
    }

    def getListOfFiles(dir: String): List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList
        } else {
            List[File]()
        }
    }

    def getEPOCHTime(date: String, time: String): Long = {
        return DateTime.parse(date + " " + PropertyReader.getProperty(time), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC()).getMillis
    }
    
    def readFile(fileName: String): RDD[EmployeeEvent] = {
        val input = sc.textFile("src/main/resources/" + fileName + "_eventsData.json")

        val result = input.mapPartitions(records => {
            // mapper object created on each executor node
            val mapper = new ObjectMapper with ScalaObjectMapper
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            mapper.registerModule(DefaultScalaModule)
            // We use flatMap to handle errors
            // by returning an empty list (None) if we encounter an issue and a
            // list with one element if everything is ok (Some(_)).
            records.flatMap(record => {
                try {
                    Some(mapper.readValue(record, classOf[EmployeeEvent]))
                } catch {
                    case e: Exception => None
                }
            })
        }, true)
        return result
    }

    def readCassendraTable: RDD[Employee] = {
        sc.cassandraTable[Employee](Contants.CONTENT_KEY_SPACE_NAME, Contants.TABLE_NAME)
    }
    def generateDateTime(date: String): DateTime = {
        DateTime.parse((date + " 09:00:00"), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC())
    }

    def getCurrentMonthFormat: String = {
        val dt = new DateTime(); // current time
        val month = dt.getYear + "-" + dt.getMonthOfYear;
        return month.toString()
    }
    
     def getMonthFormat(date: String): String = {
        println(date)
        val dt = generateDateTime(date)
        val month = dt.getYear + "-" + dt.getMonthOfYear;
        return month.toString()
    }

    def generateDate(date: DateTime): String = {
        return date.getYear + "/" + date.getMonthOfYear + "/" + date.getDayOfMonth
    }
    
    def getFileName(date: DateTime): String = {
        return date.getYear + "-" + date.getMonthOfYear + "-" + date.getDayOfMonth
    }

}