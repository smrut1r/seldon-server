package io.seldon.evaluation

import java.io.File
import java.lang.Long

import com.typesafe.config.ConfigFactory
import io.seldon.spark.SparkUtils
import io.seldon.spark.mllib.{MfConfig, MfModelCreation}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

object SeldonDataSplitter {
  val PERCENTAGE: Float = 0.8f

  val client = "ahalife";
  val folder = "/seldon-data/seldon-models/ahalife"
  val modelPath = folder + "/evaluation/inputs"
  val dataFile = "/seldon-data/seldon-models/" + client + "/actions/" + SparkUtils.getS3UnixGlob(1, 180) + "/*"

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local")
      .appName("Seldon Validation")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.driver.memory", "12g")
      .config("spark.executor.memory", "12g")
      .config("spark.driver.maxResultSize", "4g")
      //.enableHiveSupport
      .getOrCreate
    import spark.implicits._

    var c = MfConfig()
    val parser = new scopt.OptionParser[Unit]("SeldonDataSplitter") {
      opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
      opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")
    }
    parser.parse(args)
    val config = MfModelCreation.updateConf(c)
    val ratings = MfModelCreation.prepareRatings(dataFile, config, spark.sparkContext)

    //ratings.toDF().repartition(1).write.mode(SaveMode.Overwrite).json(preparedFile) //.map(r => (r.user, r.item, r.rating))

    val (train, test) = prepareSplits(PERCENTAGE, ratings.toDF("user", "item", "preference"))
    if(modelPath!=null) {
      val dir: File = new File(modelPath)
      if (!dir.exists) {
        if (!dir.mkdir) {
          println("Directory " + dir + " could not be created")
        }
      }
      /*train.repartition(1).write.mode(SaveMode.Overwrite).csv(modelPath + "train")
      test.repartition(1).write.mode(SaveMode.Overwrite).csv(modelPath + "test")*/
      val algo = "USER_BASED"
      train.repartition(1).write.mode(SaveMode.Overwrite).csv("/seldon-data/seldon-models/" + client +"/evaluation/"+ algo +"/train/")
      test.repartition(1).write.mode(SaveMode.Overwrite).csv("/seldon-data/seldon-models/" + client +"/evaluation/"+ algo +"/test/")
    }

    spark.stop()
  }

  def prepareSplits(percentage: Float, dataset: DataFrame): (DataFrame, DataFrame) = {
    if(percentage==null) return (dataset, null)
    val seed: Long = 123l

    val Array(trainDataset, testDataset) = dataset.randomSplit(Array(percentage, 1-percentage), 123)
    //val df = dataset.stat.sampleBy("user", Map(0 -> 0.1, 1 -> 0.2), 0L)
    val Array(test1, test2) = testDataset.select("userId").distinct().randomSplit(Array(0.9, 0.1), 123)
    val test = testDataset.join(test2, Seq("userId"), "leftsemi")
      //join(test2, testDataset.col("name") === test2.col("name"))

    //val splits: Array[DataModel[Long, Long]] = new RandomSplitter(percentage, perUser, seed, perItem).split(data)

    return (trainDataset, test)
  }

}
