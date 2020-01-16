package com.panzerknacker

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_set

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {


    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    val cells = inputs.map(
      spark
        .read
        .option("sep", ";")
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(_)
        .flatMap(tuple => tuple.schema.fieldNames.map(column => (tuple.getAs(column).toString, column)))
    ).reduce(_.union(_))
    println("---------------------------------------------------------------------------------------------------------")
    cells.show()
    println("---------------------------------------------------------------------------------------------------------")
    val preaggregation = cells.groupBy($"_1").agg(collect_set($"_2").as("_2"))
    println("---------------------------------------------------------------------------------------------------------")
    preaggregation.show()
    println("---------------------------------------------------------------------------------------------------------")
    val attributeSets = preaggregation.select("_2")
    println("---------------------------------------------------------------------------------------------------------")
    println(attributeSets.show())
    println("---------------------------------------------------------------------------------------------------------")
    val inclusionList = attributeSets.flatMap(row => row.getSeq[String](0).map(column => (column, row.getSeq[String](0).filter(_ != column))))
    println("---------------------------------------------------------------------------------------------------------")
    println(inclusionList.show(50))
    println("---------------------------------------------------------------------------------------------------------")
  }
}
