package com.ilimi.utils

import java.io.FileInputStream
import java.util.Properties

object PropertyReader {
    val prop = new Properties()
    try {
        prop.load(new FileInputStream("src/main/resources/appConfig.properties"))
    } catch { case e: Exception => 
      e.printStackTrace()
      sys.exit(1)
    }
    def getProperty(key: String): String = {
      return prop.getProperty(key);
    }
}