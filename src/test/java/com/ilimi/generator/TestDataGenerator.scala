package com.ilimi.generator

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.joda.time.DateTime
import org.scalatest.FlatSpec

import com.ilimi.utils.Contants
import com.ilimi.utils.Utils
import com.ilimi.utils.Employee

class TestDataGenerator extends FlatSpec {
    behavior of "Data Generator"

    it should "generate 100 employee login-logout data for given single date - 2016/12/06" in {
        val data = EmployeeDataGenerator.generate(3, "2016-12-06", true)
        assert(data.isEmpty === false)
        assert(data.length >= 3)
    }

    it should "generate 100 employee login-logout data for range of dates - startDate: 2016/12/15, endDate: 2016/12/30" in {
        EmployeeDataGenerator.bulkDataGenerator(3, "2016-12-15", "2016-12-16", true)
        assert(Utils.getListOfFiles("src/main/resources/").length - 1 >= 3)
    }

    it should "generate the employee data when all 100 employee present all days for given range of dates - startDate: 2016-12-15, endDate: 2016-12-30" in {
        EmployeeDataGenerator.bulkDataGenerator(3, "2016-12-15", "2016-12-16", true)
        val range = Utils.getAllDates("2016-12-15", "2016-12-16")
        while (range.hasNext) {
            val dt = range.next();
            val data = Utils.readFile(dt.getYear + "-" + dt.getMonthOfYear + "-" + dt.getDayOfMonth)
            val temp = data.groupBy(x => x.empId)
            assert(temp.count() == 3)
        }
    }

    it should "generate the employee data when all 100 employee is not present all days for given range of dates" in {
      EmployeeDataGenerator.bulkDataGenerator(5, "2016-10-15", "2016-10-16", false)
        val range = Utils.getAllDates("2016-10-15", "2016-10-16")
        while (range.hasNext) {
            val dt = range.next();
            val data = Utils.readFile(dt.getYear + "-" + dt.getMonthOfYear + "-" + dt.getDayOfMonth)
            val temp = data.groupBy(x => x.empId)
            assert(temp.count() <= 5)
        }
    }


}