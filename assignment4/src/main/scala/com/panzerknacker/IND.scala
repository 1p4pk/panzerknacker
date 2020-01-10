package com.panzerknacker

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object IND extends App {

  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val spark = SparkSession
      .builder()
      .appName("ind")
      .master("local[4]") // local, with 4 worker cores
      .getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types

    import spark.implicits._

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Loading data
    //------------------------------------------------------------------------------------------------------------------

    // Read a Dataset from a file
    val customer = spark.read
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("TPCH/tpch_customer.csv") // also text, json, jdbc, parquet
      .as[(Int, String, String, Int, String, Double, String, String)]

    //------------------------------------------------------------------------------------------------------------------
    // Analyzing Datasets and DataFrames
    //------------------------------------------------------------------------------------------------------------------

    customer.printSchema() // print schema of dataset/dataframe

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"TPCH/tpch_$name.csv")
  }
}