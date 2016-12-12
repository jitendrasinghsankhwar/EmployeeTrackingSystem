package com.ilimi.utils

object CaseClasses extends Serializable { }

case class EmployeeEvent(empId: String, loginTime: String, logoutTime: String)
case class Employee(empid: String, workingtime: Long, absenttime: Long, arrivaltime: Long, period: String)
case class EmpID(empid: String)
