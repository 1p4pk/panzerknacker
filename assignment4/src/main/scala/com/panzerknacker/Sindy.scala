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
      .groupBy($"_1").agg(collect_set($"_2").as("_2"))
      .select("_2")
      .flatMap(tuple => tuple.getSeq[String](0).map(
        column => (column, tuple.getSeq[String](0).filter(_ != column).toArray.toSet))
      )
      .rdd.reduceByKey((agg, i) => agg.intersect(i)).filter(_._2.size > 0)
      .collect().sortBy(_._1).map(tuple => println(tuple._1 + " < " + tuple._2.mkString(", ")))
  }
}
