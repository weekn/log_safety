package com.nfjd.util

import java.sql.DriverManager
import java.sql.Connection

object PgSqlUtil {
  def getPGConnection():Connection={
  	  val conn_str = "jdbc:postgresql://172.17.17.70:5432/nxsoc5"
			val conn = DriverManager.getConnection(conn_str, "postgres", "postgres")
			conn
  }
}