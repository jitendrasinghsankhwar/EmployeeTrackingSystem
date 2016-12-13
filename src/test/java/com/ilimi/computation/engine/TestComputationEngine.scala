package com.ilimi.computation.engine

import com.ilimi.generator.EmployeeDataGenerator
import com.ilimi.utils.Utils
import org.scalatest.FlatSpec

class TestComputationEngine extends FlatSpec {
    behavior of "Data Generator"

    it should "compute attribute 100 employee for given single date - 2016-12-06" in {
        val data = ComputationEngine.computeDrivedAttributes("2016-12-06")
        assert(data.isEmpty() === false)
        assert(data.count == 3)
    }

    it should "compute attribute 100 employee for range of dates - startDate: 2016-12-15, endDate: 2016-12-30" in {
        val range = Utils.getAllDates("2016-12-15", "2016-12-16")
        while (range.hasNext) {
            val date = range.next();
            val data = ComputationEngine.computeDrivedAttributes(date.toLocalDate().toString())
            assert(data.isEmpty() === false)
            assert(data.count == 3)
        }
    }

    it should "generate the employee data when all 100 employee present all days for given range of dates - startDate: 2016-12-15, endDate: 2016-12-30" in {
        EmployeeDataGenerator.bulkDataGenerator(100, "2016-12-15", "2016-12-16", true)
        val range = Utils.getAllDates("2016-12-15", "2016-12-16")
        while (range.hasNext) {
            val dt = range.next();
            val data = Utils.readFile(dt.getYear + "-" + dt.getMonthOfYear + "-" + dt.getDayOfMonth)
            val temp = data.groupBy(x => x.empId)
            assert(temp.count() == 3)
        }
    }

    it should "generate the employee data when all 100 employee is not present all days for given range of dates - startDate: 2016-10-15, endDate:2016-10-30" in {
        val range = Utils.getAllDates("2016-10-15", "2016-10-16")
        while (range.hasNext) {
            val date = range.next();
            val data = ComputationEngine.computeDrivedAttributes(date.toLocalDate().toString())
            assert(data.isEmpty() === false)
            assert(data.count <= 5)
        }
    }

}