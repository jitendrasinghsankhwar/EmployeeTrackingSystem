package com.ilimi.computation.engine

import scala.reflect.runtime.universe

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.joda.time.DateTime

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions
import com.ilimi.utils.Contants
import com.ilimi.utils.EmpID
import com.ilimi.utils.Employee
import com.ilimi.utils.Utils

object ComputationEngine {

    def computeDrivedAttributes(fileName: String): RDD[Employee] = {
        val data = Utils.readFile(fileName)
        val temp = data.groupBy(x => x.empId).mapValues(f => (f.map(x => x.logoutTime.toLong - x.loginTime.toLong).sum, Contants.TOTAL_WORKING_TIME - f.map(x => x.logoutTime.toLong - x.loginTime.toLong).sum, f.map(x => x.loginTime.toLong).head, f.map(x => new DateTime(x.loginTime.toLong).toLocalDate().toString()).head))
        val list = temp.map { case (a, (b, c, d, e)) => (a, b, c, d, e) }
        val employeedetails = list.map { x => Employee(x._1, x._2, x._3, x._4, x._5) }
        employeedetails.saveToCassandra(Contants.CONTENT_KEY_SPACE_NAME, Contants.TABLE_NAME)
        updateMonthDataToDB(Contants.CONTENT_KEY_SPACE_NAME, Contants.TABLE_NAME, fileName, employeedetails)
        return employeedetails
    }

    def updateMonthDataToDB(keyspaceName: String, tableName: String, fileName: String, data: RDD[Employee]) {
        val rdd = Utils.readCassendraTable
        val date = Utils.getMonthFormat(fileName)
        val filterByPeriodRDD = rdd.filter { x => date.equals(x.period) } 
        val time = rdd.filter { x => fileName.length().equals(x.period.length()) }.collect()
        
        if (filterByPeriodRDD.isEmpty()) {
            data.map { x => Employee(x.empid, x.workingtime, x.absenttime, x.arrivaltime, Utils.getMonthFormat(x.period)) }.saveToCassandra(keyspaceName, tableName)
        } else {
            val filterPeriod = filterByPeriodRDD.map { x => x.period }.first()
            val dbData = data.map { x => EmpID(x.empid) }.joinWithCassandraTable[Employee](keyspaceName, tableName).on(SomeColumns("empid")).filter { x => filterPeriod.equals(x._2.period) }
            val joinedData = data.map { x => (EmpID(x.empid), x) }.leftOuterJoin(dbData)
            val updatedData = joinedData.map { x =>
                val present = x._2._1
                val monthData = x._2._2.get
                val artTime = time.filter { y => y.empid.equals(x._1.empid) }.sortBy { f => f.arrivaltime }
                val count = artTime.length
                val temp: Int = (count/2).toInt
                val AvgArrivalTime = if(count == 1) artTime(0).arrivaltime else if(count % 2 == 0) (artTime(temp - 1).arrivaltime + artTime(temp).arrivaltime) / 2 else artTime(temp).arrivaltime
                Employee(x._1.empid, present.workingtime + monthData.workingtime, present.absenttime + monthData.absenttime, AvgArrivalTime, monthData.period)
            }
            updatedData.saveToCassandra(keyspaceName, tableName)
        }
    }

    def computeAttributeForDateRange(startDate: String, endDate: String) {
        val range = Utils.getAllDates(startDate, endDate)
        while (range.hasNext) {
            val date = range.next();
            computeDrivedAttributes(date.toLocalDate().toString())
        }
    }

    def computeAttributeForDate(date: String) {
        computeDrivedAttributes(date)
    }

    def main(args: Array[String]) {
        computeAttributeForDate("2016-12-07")
    }
}