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
        case "--path" :: value :: tail => getArgs(argsMap ++ Map('path -> value.toString), tail)
        case "--cores" :: value :: tail => getArgs(argsMap ++ Map('cores -> value.toInt) ++ Map('partitions-> value.toInt * 2), tail)
        case option :: tail =>
          println("Unkown option " + option)
          sys.exit(1)
      }
    }

    val argsList = getArgs(Map(), args.toList)
    val path = if(argsList.contains('path)) argsList('path) else "./TPCH"
    val cores = if(argsList.contains('cores)) argsList('cores) else 4
    val partitions = if(argsList.contains('partitions)) argsList('partitions) else 8

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", partitions.toString)
    spark.conf.set("spark.executor.cores", cores.toString)
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