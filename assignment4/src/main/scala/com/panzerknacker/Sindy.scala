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
    val preaggregation = cells.groupBy($"_1").agg(collect_set($"_2").as("_2"))
    val attributeSets = preaggregation.select("_2")
    val inclusionList = attributeSets.flatMap(row => row.getSeq[String](0).map(column => (column, row.getSeq[String](0).filter(_ != column).toArray.toSet)))
    val aggregate = inclusionList.rdd.reduceByKey((acc, n) => acc.intersect(n)).filter(_._2.size > 0)
    aggregate.collect().sortBy(_._1).map(row => println(row._1 + " < " + row._2.mkString(", ")))
  }
}
