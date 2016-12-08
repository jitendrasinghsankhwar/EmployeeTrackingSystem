package com.ilimi.computation.engine

import org.joda.time.DateTime

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.ilimi.utils.SparkContextObj
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import com.ilimi.utils.Utils

object ComputationEngine {
    val totalWorkingHrs = 28800000
    // For generator event object
    case class EmployeeEvent(empId: String, loginTime: String, logoutTime: String)
    
    case class Employee(empid: String, workingtime: Long, absenttime: Long, arrivaltime: Long, period: String)

    def readFile(fileName: String) {
        val sc = SparkContextObj.getSparkContext

        val input = sc.textFile("src2/main/resources/" + fileName + "_eventsData.json")
 
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

        val temp = result.groupBy(x => x.empId).mapValues(f => (f.map(x => x.logoutTime.toLong - x.loginTime.toLong).sum, totalWorkingHrs - f.map(x => x.logoutTime.toLong - x.loginTime.toLong).sum, f.map(x => x.loginTime.toLong).head, f.map(x => new DateTime(x.loginTime.toLong).toLocalDate().toString()).head))
        val list =temp.map { case (a, (b, c, d, e)) => (a, b, c, d, e) }
       
        val employeedetails = list.map{ x => Employee(x._1,x._2,x._3,x._4,x._5) }
        
        saveToDB("employeedb", "employeetablefinal", employeedetails)
//        employeedetails.map { x => Employee(x.empId, x.workingTime, x.absentTime, x.arrivalTime, x.period) };

    }
    def saveToDB(keyspaceName: String, tableName: String, data: RDD[Employee]) {
        data.saveToCassandra(keyspaceName, tableName)
    }
    
    def computeAttributeForDateRange(startDate: String, endDate: String) {
        val range = Utils.getAllDates(startDate, endDate)
        while (range.hasNext) {
            val date = range.next();
            readFile(date.getYear + "/" + date.getMonthOfYear + "/" + date.getDayOfMonth)
        }
    }
    def computeAttributeForDate(date: String) {
        readFile(date)
    }
    def main(args: Array[String]) {
        computeAttributeForDate("2016-12-02")
    }
}