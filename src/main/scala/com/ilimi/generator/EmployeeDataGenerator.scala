package com.ilimi.generator
import scala.util.Random
import scala.collection.mutable.ListBuffer
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.commons.math.random.RandomData
import org.apache.commons.math.random.RandomDataImpl
import org.json4s.native.Json
import org.json4s.DefaultFormats
import java.io.File
import java.io.PrintWriter
import java.io.FileInputStream
import com.ilimi.utils.PropertyReader
import com.ilimi.utils.Utils
import org.joda.time.Period

object EmployeeDataGenerator {
    val MIN_ARRIVAL_TIME = "minArrivalTime"
    val MAX_ARRIVAL_TIME = "maxArrivalTime"
    val MAX_LOGOUT_TIME = "maxLogoutTime"

    def dataGenerator(employeeCount: Int, date: String) {
        val listOfEmployeeEvents = new ListBuffer[scala.collection.mutable.Map[String, String]]()
        val employeeIdList = scala.collection.mutable.Map[String, String]();

        def generateEmployeeIds(employeeCount: Int) {
            val random = new Random()
            val dt = Utils.generateDateTime(date)
            for (employee <- 1 to employeeCount) {
                val empId: String = "emp_" + (31800 + employee)
                generateEmployeeEvents(empId, DateTime.parse(date + " " + PropertyReader.getProperty(MIN_ARRIVAL_TIME), DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")).getMillis, DateTime.parse(date + " " + PropertyReader.getProperty(MAX_ARRIVAL_TIME), DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")).getMillis)
            }
            if (!new java.io.File(PropertyReader.getProperty("path") + dt.toLocalDate().toString() + "_eventsData.json").exists) {
                val writer = new PrintWriter(new File(PropertyReader.getProperty("path") + dt.toLocalDate().toString() + "_eventsData.json"))
                try {
                    listOfEmployeeEvents.map { x =>
                        println(Json(DefaultFormats).write(x))
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

        // Generate each employee daily login, logout events
        def generateEmployeeEvents(employeeId: String, minArrivalTime: Long, maxArrivalTime: Long) {
            val maxLogoutTime = DateTime.parse(date + " " + PropertyReader.getProperty(MAX_LOGOUT_TIME), DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")).getMillis
            val rand: RandomData = new RandomDataImpl()

            // Generate epoch time using given date time format in string
            def generateMultipleLoginLogouts(minArrivalTime: Long, maxArrivalTime: Long) {
                if ((maxArrivalTime + 1000) >= maxLogoutTime) return
                val loginTime = rand.nextLong(minArrivalTime, maxArrivalTime + 1);
                val logoutTime = rand.nextLong(maxArrivalTime, maxLogoutTime + 1);
                listOfEmployeeEvents += scala.collection.mutable.Map[String, String]("empId" -> employeeId, "loginTime" -> loginTime.toString(), "logoutTime" -> logoutTime.toString())
                //if(logoutTime + 6200 <= maxLogoutTime) generateMultipleLoginLogouts(logoutTime, maxLogoutTime) else generateMultipleLoginLogouts(logoutTime, maxLogoutTime)
                generateMultipleLoginLogouts(logoutTime, logoutTime + 7200000)
            }
            generateMultipleLoginLogouts(minArrivalTime, maxArrivalTime)
        }
        generateEmployeeIds(employeeCount)
    }

    def bulkDataGenerator() {
        val startDate = PropertyReader.getProperty("startDate")
        val endDate = PropertyReader.getProperty("endDate")
        val range = Utils.getAllDates(startDate, endDate)
        while (range.hasNext) {
            val date = range.next();
            dataGenerator(20, date.getYear + "/" + date.getMonthOfYear + "/" + date.getDayOfMonth)
        }
    }
    def main(args: Array[String]) {
        //    dataGenerator(20, "2016/12/05")
        bulkDataGenerator()

    }
}