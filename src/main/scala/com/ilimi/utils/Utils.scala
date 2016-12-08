package com.ilimi.utils
import org.joda.time.Period
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object Utils {
  def getAllDates(startDate: String, endDate: String): Iterator[DateTime] = {
    def dateRange(start: DateTime, end: DateTime, step: Period): Iterator[DateTime] = Iterator.iterate(start)(_.plus(step)).takeWhile(!_.isAfter(end))
    val range = dateRange( DateTime.parse((startDate + " 09:00:00"), DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")), DateTime.parse(( endDate + " 09:00:00"), DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")), Period.days(1))
    return range 
  }
  
  def generateDateTime(date: String): DateTime = {
    return DateTime.parse((date + " 09:00:00"), DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss"))
  }
}