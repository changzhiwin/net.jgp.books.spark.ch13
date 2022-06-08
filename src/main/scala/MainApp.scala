package net.jgp.books.spark

import net.jgp.books.sparkInAction.ch13.lab110_flatten_shipment.FlattenShipmentDisplay
import net.jgp.books.sparkInAction.ch13.lab120_restaurant_document.RestaurantDocument

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("_", "")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    whichCase match {
      case "FLAT"     => FlattenShipmentDisplay.run()
      case _          => RestaurantDocument.run()
    }
  }
}