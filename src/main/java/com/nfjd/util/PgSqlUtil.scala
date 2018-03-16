package com.nfjd.util

import java.sql.DriverManager
import java.sql.Connection
import scala.collection.mutable.MutableList
import scala.collection.mutable.Queue
import java.sql.ResultSet
import java.sql.Timestamp

object PgSqlUtil {

	private val max_connection = 20 //连接池总数
	private val connection_num = 20 //产生连接数
	private var current_num = 0 //当前连接池已产生的连接数
	private val pools = new Queue[Connection]() //连接池

	private val url = "jdbc:postgresql://172.17.17.70:5432/nxsoc5"
	private val username = "postgres"
	private val password = "postgres"
	def main(args: Array[String]): Unit = {
		val conn = getPGConnection()
		val name_type_mapping = Map(
			"recordid" -> "string",
			"SRCIP" -> "string",
			"DSTIP" -> "string",
			"ATTACKTYPE" -> "string",
			"RECORDTIME" -> "time",
			"COSTTIME" -> "double",
			"SRCPORT" -> "int",
			"DSTPORT" -> "int",
			"PROTO" -> "string",
			"PACKERNUM" -> "int",
			"BYTESIZE" -> "int",
			"FLOWNUMA" -> "int",
			"FLOWNUMS" -> "int",
			"FLOWNUMD" -> "int"
		)
		val data_map = Map(
			"recordid" -> "test",
			"SRCIP" -> "192,19,29.32",
			"DSTIP" -> "192,19,29.32",
			"ATTACKTYPE" -> "dos",
			"RECORDTIME" -> 1521181465000L.toLong,
			"COSTTIME" -> 1521181465000L.toDouble,
			"SRCPORT" -> 4244,
			"DSTPORT" -> 4244,
			"PROTO" -> "tcp",
			"PACKERNUM" -> 3,
			"BYTESIZE" -> 2,
			"FLOWNUMA" -> 1,
			"FLOWNUMS" -> 4,
			"FLOWNUMD" -> 1

		)
		val insert = Map("recordid" -> "dsad", "status" -> 42)
		val insert_data: Array[Map[String, Any]] = Array()
		insertBatch(conn, "t_siem_netflow_rfpredict", name_type_mapping, insert_data)

	}
	def getPGConnection(): Connection = {
		DriverManager.getConnection(url, username, password)
	}

	def insertBatch(conn: Connection, tableName: String, name_type_Mapping: Map[String, String], insert_data: Array[Map[String, Any]]): Unit = {

		//		val name_type_Mapping=Map("recordid"->"string","status"->"int")

		//		val insert_data1=Map("recordid"->"weekn","status"->432)
		//		val insert_data2=Map("recordid"->"ffok","status"->432)
		//
		//
		//		val insert_data=Array(insert_data1,insert_data2)
		//build sql

		val name_type_Mapping_key_arr = name_type_Mapping.keySet.toArray
		val size_name_type_Mapping_key_arr = name_type_Mapping_key_arr.size
		val insert_data_size = insert_data.size
		if (insert_data_size > 1) {
			val sql_buf = new StringBuilder("""INSERT INTO public.""" + tableName)
			val sql_insert_name = new StringBuilder("""(""")
			val sql_insert_v = new StringBuilder("""(""")
			for (i <- 0 until size_name_type_Mapping_key_arr) {

				sql_insert_name ++= name_type_Mapping_key_arr.apply(i)
				sql_insert_v ++= "?"
				if (i != size_name_type_Mapping_key_arr - 1) {
					sql_insert_name ++= ","
					sql_insert_v ++= ","
				} else {
					sql_insert_name ++= ")"
					sql_insert_v ++= ")"
				}

			}
			sql_buf ++= sql_insert_name ++= """ VALUES """

			for (i <- 0 until insert_data_size) {
				sql_buf ++= sql_insert_v
				if (i != insert_data_size - 1) {
					sql_buf ++= ","
				} else {
					sql_buf ++= ";"
				}
			}
			println(sql_buf)

			//设置数值--------------------------------
			val prep = conn.prepareStatement(sql_buf.toString())
			for (i <- 0 until insert_data_size) {
				val data = insert_data.apply(i)
				for (j <- 0 until size_name_type_Mapping_key_arr) {
					val name = name_type_Mapping_key_arr.apply(j)
					val v = data.apply(name)
					val index = i * size_name_type_Mapping_key_arr + j + 1
					name_type_Mapping.apply(name) match {
						case "string" => prep.setString(index, v.asInstanceOf[String])
						case "int" => prep.setInt(index, v.asInstanceOf[Int])
						case "time" => {

							prep.setTimestamp(index, new Timestamp(v.asInstanceOf[Long]))
						}
						case "double" => {
							prep.setDouble(index, v.asInstanceOf[Double])
						}
					}

				}
			}

			// excute---------

			prep.executeUpdate
		}

	}

	/**
	 * 初始化连接池
	 */
	private def initConnectionPool(): Queue[Connection] = {
		AnyRef.synchronized({
			if (pools.isEmpty) {
				//
				for (i <- 1 to connection_num.toInt) {
					pools += getPGConnection()

					current_num += 1
				}
			}
			pools
		})
	}
	/**
	 * 获得连接
	 */
	def getConn(): Connection = {
		initConnectionPool()
		pools.dequeue()
	}
	/**
	 * 释放连接
	 */
	def releaseCon(con: Connection) {
		pools += con
	}
	def destory() {
		val iter = pools.toIterator
		while (iter.hasNext) {
			iter.next().close()
		}
	}
}