package com.ilimi.computation.engine

import org.apache.spark.rdd.RDD
import com.ilimi.utils.Contants
import org.joda.time.DateTime

import com.datastax.spark.connector._
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.ilimi.utils.SparkContextObj
import com.ilimi.utils.Utils
import com.ilimi.utils.EmpID
import com.ilimi.utils.Employee
import com.ilimi.utils.EmployeeEvent

object ComputationEngine {

    val sc = SparkContextObj.getSparkContext
    def readFile(fileName: String) {

        val input = sc.textFile("src/main/resources/" + fileName + "_eventsData.json")
        //val result = Utils.loadFile[EmployeeEvent]("src/main/resources/" + fileName + "_eventsData.json");

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

        val temp = result.groupBy(x => x.empId).mapValues(f => (f.map(x => x.logoutTime.toLong - x.loginTime.toLong).sum, Contants.TOTAL_WORKING_TIME - f.map(x => x.logoutTime.toLong - x.loginTime.toLong).sum, f.map(x => x.loginTime.toLong).head, f.map(x => new DateTime(x.loginTime.toLong).toLocalDate().toString()).head))
        val list = temp.map { case (a, (b, c, d, e)) => (a, b, c, d, e) }
        val employeedetails = list.map { x => Employee(x._1, x._2, x._3, x._4, x._5) }
        saveToDb(Contants.CONTENT_KEY_SPACE_NAME, Contants.TABLE_NAME, employeedetails)
        updateToDb(Contants.CONTENT_KEY_SPACE_NAME, Contants.TABLE_NAME, employeedetails)
    }
    def saveToDb(keyspaceName: String, tableName: String, data: RDD[Employee]) {
        data.saveToCassandra(keyspaceName, tableName)
    }
    def updateToDb(keyspaceName: String, tableName: String, data: RDD[Employee]) {
        val dbData = data.map { x => EmpID(x.empid) }.joinWithCassandraTable[Employee](keyspaceName, tableName).on(SomeColumns("empid")).filter { x => Utils.getMonth.equals(x._2.period) }
        val joinedData = data.map { x => (EmpID(x.empid), x) }.leftOuterJoin(dbData)
         val updatedData = joinedData.map{ x => 
             val present = x._2._1
             val monthData = x._2._2.get
             // TODO calculate arrivaltime for the period
             Employee(x._1.empid, present.workingtime + monthData.workingtime, present.absenttime + monthData.absenttime, monthData.arrivaltime, monthData.period) 
        }
        updatedData.saveToCassandra(keyspaceName, tableName)
        
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
        computeAttributeForDate("2016-12-03")
    }
}