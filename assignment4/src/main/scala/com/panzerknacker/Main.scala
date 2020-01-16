package com.panzerknacker

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {

  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkSession = SparkSession
      .builder()
      .appName("ind")
      .master("local[4]") // local, with 4 worker cores

    val spark = sparkSession.getOrCreate()

    // Read command-line arguments
    type ArgsMap = Map[Symbol, Any]

    def getArgs(argsMap: ArgsMap, list: List[String]) : ArgsMap = {
      list match {
        case Nil => argsMap
        case "--path" :: value :: tail => getArgs(argsMap ++ Map('path -> value), tail)
        case "--cores" :: value :: tail => getArgs(argsMap ++ Map('cores -> value.toInt), tail)
        case option :: tail =>
          println("Unkown option " + option)
          sys.exit(1)
      }
    }

    val argsList = getArgs(Map(), args.toList)
    val path = "TPCH"
    val cores = 4

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types

    println("---------------------------------------------------------------------------------------------------------")

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }
    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"$path/tpch_$name.csv")

    time {
      Sindy.discoverINDs(inputs, spark)
    }
  }
}