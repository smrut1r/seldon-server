package io.seldon.evaluation

import java.lang.Long

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import com.github.jongwook.SparkRankingMetrics
import org.apache.spark.ml.evaluation.RegressionEvaluator


object SeldonEvaluator {

  val ATS = Array[Int](5, 10, 20)
  val client = "ahalife"

  val testSchema = StructType(Seq(StructField("userId", LongType, false), StructField("itemId", LongType, false), StructField("rating", DoubleType, false)))
  val predSchema = StructType(Seq(StructField("userId", LongType, false), StructField("itemId", LongType, false), StructField("prediction", DoubleType, false)))

  val conf = new SparkConf().setAppName("Seldon Validation").setMaster("local[2]")
    .set("spark.driver.memory", "30g")
    .set("spark.executor.memory", "30g")
    .set("spark.driver.maxResultSize", "10g")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  def evaluate(algo: String, testData: DataFrame, recData: DataFrame) {

    val maeEvaluator = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("prediction")
      .setMetricName("mae")

    val rmseEvaluator = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val testRec = testData.join(recData, Seq("userId", "itemId"))
    //testRec.repartition(1).write.mode(SaveMode.Overwrite).csv(recPath + algo+"_test")
    println("Join size " + testRec.count())

    val metrics = SparkRankingMetrics(recData, testData)
    metrics.setUserCol("userId")
    metrics.setItemCol("itemId")
    metrics.setRatingCol("rating")
    metrics.setPredictionCol("prediction")

    /*val pStRecall: PopularityStratifiedRecall[Long, Long] = new PopularityStratifiedRecall[Long, Long](recModel, testModel, REL_TH, Array[Int](AT5,AT10,AT20))
    pStRecall.compute*/
    println("-------------------------")
    println(algo + "- MAE: " + maeEvaluator.evaluate(testRec))
    println(algo + "- RMSE: " + rmseEvaluator.evaluate(testRec))


    ATS.foreach(AT => {
      println("-------------------------")
      println(algo + "- NDCG@" + AT + ": " + metrics.ndcgAt(AT))
      //println(algo+"- MAP@" + AT + ": " + metrics.mapAt(AT))
      println(algo + "- MRR@" + AT + ": " + metrics.mrrAt(AT))
      println(algo + "- Precision@" + AT + ": " + metrics.precisionAt(AT))
      println(algo + "- Recall@" + AT + ": " + metrics.recallAt(AT))
      println(algo + "- F1@" + AT + ": " + metrics.f1At(AT))
    })
  }

  def main(args: Array[String]) {
    val algos = Array(
      "USER_BASED",
      "RECENT_MATRIX_FACTOR" //,
      //"RECENT_SIMILAR_ITEMS",
      //"RECENT_TOPIC_MODEL",
      //"WORD2VEC"
    )
    algos.map(algo => {
      val testData = spark.read.option("header", "false").schema(testSchema).csv("/seldon-data/seldon-models/" + client +"/evaluation/"+ algo +"/test/")
      val recData = spark.read.option("header", "false").schema(predSchema).csv("/seldon-data/seldon-models/" + client +"/evaluation/"+ algo +"/pred/")
      evaluate(algo, testData, recData)
    })
    println("completed!!")
    spark.stop()
  }
}