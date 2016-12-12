package com.ilimi.utils
import org.joda.time.Period
import com.fasterxml.jackson.core.`type`.TypeReference
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.fasterxml.jackson.databind.ObjectMapper
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type


object Utils {
  @transient val mapper = new ObjectMapper();
  implicit var sc: SparkContext = null;
  
  def getAllDates(startDate: String, endDate: String): Iterator[DateTime] = {
    def dateRange(start: DateTime, end: DateTime, step: Period): Iterator[DateTime] = Iterator.iterate(start)(_.plus(step)).takeWhile(!_.isAfter(end))
    val range = dateRange( DateTime.parse((startDate + " 09:00:00"), DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")), DateTime.parse(( endDate + " 09:00:00"), DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")), Period.days(1))
    return range 
  }
  
  def generateDateTime(date: String): DateTime = {
    return DateTime.parse((date + " 09:00:00"), DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss"))
  }
  
  def getMonth: String = {
      val dt = new DateTime();  // current time
      val month = dt.getYear + "-" + dt.getMonthOfYear;
      return month.toString()
  }
  def loadFile[T](file: String)(implicit mf: Manifest[T]): RDD[T] = {
      if (file == null) {
          return null;
      }
      sc.textFile(file, 1).map { line => deserialize[T](line) }.filter { x => x != null }.cache();
  }
  
  private[this] def typeReference[T: Manifest] = new TypeReference[T] {
      override def getType = typeFromManifest(manifest[T])
  }
  
  @throws(classOf[Exception])
  def deserialize[T: Manifest](value: String): T = mapper.readValue(value, typeReference[T]);
  
  private[this] def typeFromManifest(m: Manifest[_]): Type = {
        if (m.typeArguments.isEmpty) { m.runtimeClass }
        // $COVERAGE-OFF$Disabling scoverage as this code is impossible to test
        else new ParameterizedType {
            def getRawType = m.runtimeClass
            def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
            def getOwnerType = null
        }
        // $COVERAGE-ON$
    }
}