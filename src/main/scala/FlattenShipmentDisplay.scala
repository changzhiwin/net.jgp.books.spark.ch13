package net.jgp.books.sparkInAction.ch13.lab110_flatten_shipment

import org.apache.spark.sql.functions.{explode}

import net.jgp.books.spark.basic.Basic

object FlattenShipmentDisplay extends Basic {

  def run(): Unit = {
    val spark = getSession("Flatenning JSON doc describing shipments")

    val df = spark.read.
      format("json").
      option("multiline", "true").
      load("data/json/shipment.json")
    df.show(3)
    df.printSchema

    import spark.implicits._
    val flatDF = df.withColumn("items", explode($"books")).
      withColumn("supplier_city", $"supplier.city").
      withColumn("supplier_country", $"supplier.country").
      withColumn("supplier_name", $"supplier.name").
      withColumn("supplier_state", $"supplier.state").
      drop("supplier").
      withColumn("customer_city", $"customer.city").
      withColumn("customer_country", $"customer.country").
      withColumn("customer_name", $"customer.name").
      withColumn("customer_state", $"customer.state").
      drop("customer")
    flatDF.show(3)
    flatDF.printSchema

    val cleanDF = flatDF.withColumn("qty", $"items.qty").withColumn("title", $"items.title").
      drop("items").drop("books")

    cleanDF.show(3)
    cleanDF.printSchema

    cleanDF.createOrReplaceTempView("shipment_detail")
    val bookCountDF = spark.sql("select count(*) as titleCount from shipment_detail")

    bookCountDF.show(3)
    bookCountDF.printSchema

    spark.close()
  }
}