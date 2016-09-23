/*
package io.seldon.mahout


import java.util
import java.util.logging.Logger

import org.apache.mahout.math.cf.SimilarityAnalysis
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.json4s
import org.json4s.JsonAST
import org.json4s.JsonAST._

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration.Duration
import org.apache.spark.SparkContext
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.convert.wrapAsScala._
import scala.concurrent.duration.Duration

/** Setting the option in the params case class doesn't work as expected when the param is missing from
  * engine.json so set these for use in the algorithm when they are not present in the engine.json
  */
object defaultURAlgorithmParams {
  val DefaultMaxEventsPerEventType = 500
  val DefaultNum = 20
  val DefaultMaxCorrelatorsPerEventType = 50
  val DefaultMaxQueryEvents = 100 // default number of user history events to use in recs query

  val DefaultExpireDateName = "expireDate" // default name for the expire date property of an item
  val DefaultAvailableDateName = "availableDate" //defualt name for and item's available after date
  val DefaultDateName = "date" // when using a date range in the query this is the name of the item's date
  val DefaultRecsModel = "all" // use CF + backfill
  val DefaultBackfillParams = BackfillField()
  val DefaultBackfillFieldName = "popRank"
  val DefaultBackfillType = "popular"
  val DefaultBackfillDuration = "3650 days" // for all time
}


case class BackfillField(
                          name: Option[String] = None,
                          backfillType: Option[String] = None, // may be 'hot', or 'trending' also
                          eventNames: Option[List[String]] = None, // None means use the algo eventNames list, otherwise a list of events
                          offsetDate: Option[String] = None, // used only for tests, specifies the offset date to start the duration so the most
                          // recent date for events going back by from the more recent offsetDate - duration
                          duration: Option[String] = None) // duration worth of events
// to use in calculation of backfill

case class URAlgorithmParams()

class URAlgorithm(val ap: URAlgorithmParams) {

  case class BoostableCorrelators(actionName: String, itemIDs: Seq[String], boost: Option[Float])
  case class FilterCorrelators(actionName: String, itemIDs: Seq[String])

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data:Any): Any = {

    val dateNames = Some(List(ap.dateName.getOrElse(""), ap.availableDateName.getOrElse(""),
      ap.expireDateName.getOrElse(""))) // todo: return None if all are empty?
    //logger.info(s"backfill: ${ap.backfillField.toString}")
    val backfillFieldName = ap.backfillField.getOrElse(BackfillField()).name
        .getOrElse(defaultURAlgorithmParams.DefaultBackfillFieldName)

    ap.recsModel.getOrElse(defaultURAlgorithmParams.DefaultRecsModel) match {
      case "all" => calcAll(sc, data, dateNames, backfillFieldName)
      case "collabFiltering" => calcAll(sc, data, dateNames, backfillFieldName, popular = false )
      case "backfill" => calcPop(sc, data, dateNames, backfillFieldName)
      // error, throw an exception
      case _ => throw new IllegalArgumentException("Bad recsModel in engine definition params, possibly a bad json value.")
    }
  }

  /** Calculates recs model as well as popularity model */
  def calcAll(
               sc: SparkContext,
               dateNames: Option[List[String]] = None,
               backfillFieldName: String,
               popular: Boolean = true):



}*/
