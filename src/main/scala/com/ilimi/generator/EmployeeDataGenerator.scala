package com.ilimi.generator

import java.io.File
import java.io.PrintWriter

import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.commons.math.random.RandomData
import org.apache.commons.math.random.RandomDataImpl
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.DefaultFormats
import org.json4s.native.Json

import com.ilimi.utils.Contants
import com.ilimi.utils.PropertyReader
import com.ilimi.utils.Utils

object EmployeeDataGenerator {

    def generate(employeeCount: Int, date: String, allPresent: Boolean): ListBuffer[scala.collection.mutable.Map[String, String]] = {
        val listOfEmployeeEvents = new ListBuffer[scala.collection.mutable.Map[String, String]]()
        val employeeIdList = scala.collection.mutable.Map[String, String]();
        
        def generateRandomEmployeeStrenth(count: Int): Int = {
            val start = count % 10
            val end = count
            val rnd = new scala.util.Random
            return start + rnd.nextInt((end - start) + 1)
        }
        val count = if(allPresent) employeeCount else generateRandomEmployeeStrenth(employeeCount)
        println(count)
        def generateEmployeeIds(employeeCount: Int) {
            val random = new Random()
            for (employee <- 1 to count) {
                val empId: String = "emp_" + (31800 + employee)
                generateEmployeeEvents(empId, Utils.getEPOCHTime(date, Contants.MIN_ARRIVAL_TIME), Utils.getEPOCHTime(date, Contants.MAX_ARRIVAL_TIME))
            }
            saveToFile
        }

        // Generate each employee daily login, logout events
        def generateEmployeeEvents(employeeId: String, minArrivalTime: Long, maxArrivalTime: Long) {
            val maxLogoutTime = Utils.getEPOCHTime(date, Contants.MAX_LOGOUT_TIME)
            val rand: RandomData = new RandomDataImpl()

            // Generate epoch time using given date time format in string
            def generateMultipleLoginLogouts(minArrivalTime: Long, maxArrivalTime: Long) {
                if ((maxArrivalTime + 1000) >= maxLogoutTime) return
                val loginTime = rand.nextLong(minArrivalTime, maxArrivalTime + 1);
                val logoutTime = rand.nextLong(maxArrivalTime, maxLogoutTime + 1);
                listOfEmployeeEvents += scala.collection.mutable.Map[String, String]("empId" -> employeeId, "loginTime" -> loginTime.toString(), "logoutTime" -> logoutTime.toString())
                // if(logoutTime + 6200 <= maxLogoutTime) generateMultipleLoginLogouts(logoutTime, maxLogoutTime) else generateMultipleLoginLogouts(logoutTime, maxLogoutTime)
                generateMultipleLoginLogouts(logoutTime, logoutTime + 7200000)
            }
            generateMultipleLoginLogouts(minArrivalTime, maxArrivalTime)
        }

        def saveToFile {
            val dt = Utils.generateDateTime(date)
            if (!new java.io.File(PropertyReader.getProperty("path") + dt.toLocalDate().toString() + "_eventsData.json").exists) {
                val writer = new PrintWriter(new File(PropertyReader.getProperty("path") + dt.toLocalDate().toString() + "_eventsData.json"))
                try {
                    listOfEmployeeEvents.map { x =>
                        //println(Json(DefaultFormats).write(x))
                        writer.write(Json(DefaultFormats).write(x))
                        writer.write("\n")
                    }
                } catch {
                    case e: Exception =>
                        e.printStackTrace()
                        sys.exit(1)
                } finally {
                    writer.close()
                    println("Writing to JSON file Done!!")
                }
            } else {
                println("file already present in location !!! : " + dt.toLocalDate().toString() + "_eventsData.json")
            }
        }
        generateEmployeeIds(employeeCount)
        return listOfEmployeeEvents

    }

    def generateDataForRange(employeeCount: Int, range: Iterator[DateTime], allPresent: Boolean) = {
        while (range.hasNext) {
            val date = range.next();
            generate(employeeCount, date.toLocalDate().toString(), allPresent)
        }
    }
    
    def bulkDataGenerator(employeeCount: Int, allPresent: Boolean) {
        generateDataForRange(employeeCount, Utils.getAllDates(PropertyReader.getProperty("startDate"), PropertyReader.getProperty("endDate")), allPresent)
    }

    def bulkDataGenerator(employeeCount: Int, startDate: String, endDate: String, allPresent: Boolean) {
        generateDataForRange(employeeCount, Utils.getAllDates(startDate, endDate), allPresent)
    }

    def main(args: Array[String]) {
//        generate(20, "2016-12-05", true)
          bulkDataGenerator(20, true)
//        println(Utils.getListOfFiles("src/main/resources/").length);

    }
}