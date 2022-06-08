package net.jgp.books.sparkInAction.ch13.lab120_restaurant_document

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, struct}
import com.typesafe.scalalogging.Logger

import net.jgp.books.spark.basic.Basic

object RestaurantDocument extends Basic {

  val log = Logger(getClass.getName)

  def run(): Unit = {
    val spark = getSession("Building a restaurant fact sheet")

    val businessDF = spark.read.
      format("csv").
      option("header", "true").
      load("data/orangecounty_restaurants/businesses.CSV")
    businessDF.show(5)
    businessDF.printSchema
    log.info("-------> Get businessDF count: {}", businessDF.count)

    val inspectionDF = spark.read.
      format("csv").
      option("header", "true").
      load("data/orangecounty_restaurants/inspections.CSV")
    inspectionDF.show(5)
    inspectionDF.printSchema
    log.info("-------> Get inspectionDF count: {}", inspectionDF.count)

    val joinDF = businessDF.join(inspectionDF, businessDF("business_id") === inspectionDF("business_id"), "inner")
    joinDF.show(5)
    joinDF.printSchema
    log.info("-------> Get joinDF count: {}", joinDF.count)

    val tempDF = joinDF.withColumn("temp_column", struct(getColumnsOfDataFrame(inspectionDF):_*))
    tempDF.show(5)
    tempDF.printSchema
    log.info("-------> Get tempDF count: {}", tempDF.count)

    import spark.implicits._
    val nestedDF = tempDF.groupBy(getColumnsOfDataFrame(businessDF):_*).agg(collect_list($"temp_column").as("inspections"))
    nestedDF.show(5)
    nestedDF.printSchema
    log.info("-------> Get nestedDF count: {}", nestedDF.count)

    spark.close
  }

  private def getColumnsOfDataFrame(df: DataFrame): Seq[Column] = {
    val colStrings = df.columns
    colStrings.map(c => df.col(c))
  }

}