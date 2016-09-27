/*
 * Copyright 2015 recommenders.net.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.seldon.mahout

import java.io.File
import java.lang.Long
import java.sql.{DriverManager, ResultSet}

import akka.actor.{ActorRef, ActorSystem, Inbox, Props}
import com.typesafe.config.ConfigFactory
import io.seldon.spark.SparkUtils
import io.seldon.spark.zookeeper.ZkCuratorHandler
import org.apache.curator.utils.EnsurePath
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._

import scala.concurrent.{Await, Promise}


case class ActionWeightings (actionMapping: List[ActionWeighting])

case class ActionWeighting (actionType:Int = 1,
                            valuePerAction:Double = 1.0,
                            maxSum:Double = 1.0
                           )
case class MahoutConfig(
           client : String = "",
           inputPath : String = "/seldon-models",
           outputPath : String = "/seldon-models",
           startDay : Int = 1,
           days : Int = 1,
           awsKey : String = "",
           awsSecret : String = "",
           local : Boolean = false,
           zkHosts : String = "",
           activate : Boolean = false,
           actionWeightings: Option[List[ActionWeighting]] = None
         )


object MahoutModelCreation {

  val appConfig = ConfigFactory.parseResourcesAnySyntax("env").withFallback(ConfigFactory.parseResourcesAnySyntax("application"))
  val config = ConfigFactory.load(appConfig)
  //val config = ConfigFactory.load("application.conf")

  /*val conf = new SparkConf().setAppName("Seldon Validation").setMaster("local[2]")
    .set("spark.driver.memory", "12g")
    .set("spark.executor.memory", "12g")
    .set("spark.driver.maxResultSize", "4g")
  val sc = new SparkContext(conf)
  val spark = new SQLContext(sc)*/

    def updateConf(config : MahoutConfig) =
    {
      var c = config.copy()
      if (config.zkHosts.nonEmpty)
      {
        val curator = new ZkCuratorHandler(config.zkHosts)
        val path = "/all_clients/"+config.client+"/offline/mahout"
        if (curator.getCurator.checkExists().forPath(path) != null)
        {
          val bytes = curator.getCurator.getData().forPath(path)
          val j = new String(bytes,"UTF-8")
          println("Configuration from zookeeper -> ",j)
          import org.json4s._
          import org.json4s.jackson.JsonMethods._
          implicit val formats = DefaultFormats
          val json = parse(j)
          import org.json4s.JsonDSL._
          import org.json4s.jackson.Serialization.write
          type DslConversion = MahoutConfig => JValue
          val existingConf = write(c) // turn existing conf into json
        val existingParsed = parse(existingConf) // parse it back into json4s internal format
        val combined = existingParsed merge json // merge with zookeeper value
          c = combined.extract[MahoutConfig] // extract case class from merged json
          c
        }
        else
        {
          println("Warning: using default configuration - path["+path+"] not found!");
          c
        }
      }
      else
      {
        println("Warning: using default configuration - no zkHost!");
        c
      }
    }

    def prepareRatings(glob:String, config:MahoutConfig, sc:SparkContext): RDD[Rating[Int]] = {
      val actionWeightings = config.actionWeightings.getOrElse(List(ActionWeighting()))
      // any action not mentioned in the weightings map has a default score of 0.0
      val weightingsMap = actionWeightings.map(
        aw => (aw.actionType, (aw.valuePerAction, aw.maxSum))
      ).toMap.withDefaultValue(.0, .0)

      println("Using weightings map" + weightingsMap)
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      // set up environment

      println("Looking at " + glob)
      val actions: RDD[((Int, Int), Int)] = sc.textFile(glob)
        .map { line =>
          val json = parse(line)
          import org.json4s._
          implicit val formats = DefaultFormats
          val user = (json \ "userid").extract[Int]
          val item = (json \ "itemid").extract[Int]
          val actionType = (json \ "type").extract[Int]
          ((item, user), actionType)
        }.repartition(2).cache()

      // group actions by user-item key
      val actionsByType: RDD[((Int, Int), List[Int])] = actions.combineByKey((x: Int) => List[Int](x),
        (list: List[Int], y: Int) => y :: list,
        (l1: List[Int], l2: List[Int]) => l1 ::: l2)
      // count the grouped actions by action type
      val actionsByTypeCount: RDD[((Int, Int), Map[Int, Int])] = actionsByType.map {
        case (key, value: List[Int]) => (key, value.groupBy(identity).mapValues(_.size).map(identity))
      }

      // apply weigtings map
      val actionsByScore: RDD[((Int, Int), Double)] = actionsByTypeCount.mapValues {
        case x: Map[Int, Int] => x.map {
          case y: (Int, Int) => Math.min(weightingsMap(y._1)._1 * y._2, weightingsMap(y._1)._2)
        }.reduce(_ + _)
      }

      actionsByScore.take(10).foreach(println)
      val itemsByCount: RDD[(Int, Int)] = actionsByScore.map(x => (x._1._1, 1)).reduceByKey(_ + _)
      val itemsCount = itemsByCount.count()
      println("total actions " + actions.count())
      println("total actions normalized" + actionsByScore.count())
      println("total items " + itemsByCount.count())

      val usersByCount = actionsByScore.map(x => (x._1._2, 1)).reduceByKey(_ + _)
      val usersCount = usersByCount.count()
      println("total users " + usersCount)

      val ratings = actionsByScore.map {
        case ((product, user), rating) => Rating(user, product, rating.toFloat)
      }.repartition(2).cache()

      ratings
    }

  def main(args: Array[String]) {

    /*spark.read.json(dataFile).createOrReplaceTempView("actions")
    val actionData = spark.sql("SELECT userid, itemid, type, timestamp_utc FROM actions")
    actionData.repartition(1).write.mode(SaveMode.Overwrite).json(preparedFile)*/

    var c = new MahoutConfig()
    val parser = new scopt.OptionParser[Unit]("SeldonDataSplitter") {
      opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
      opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")
      opt[String]('i', "inputPath") valueName("path url") foreach { x => c = c.copy(inputPath = x) } text("path prefix for input")
      opt[String]('o', "outputPath") valueName("path url") foreach { x => c = c.copy(outputPath = x) } text("path prefix for output")
      opt[Int]('r', "days") foreach { x =>c = c.copy(days = x) } text("number of days in past to get foreachs for")
      opt[Int]("startDay") foreach { x =>c = c.copy(startDay = x) } text("start day in unix time")
      opt[String]('a', "awskey") valueName("aws access key") foreach { x => c = c.copy(awsKey = x) } text("aws key")
      opt[String]('s', "awssecret") valueName("aws secret") foreach { x => c = c.copy(awsSecret = x) } text("aws secret")
      opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")
      opt[Unit]("activate") foreach { x => c = c.copy(activate = true) } text("activate the model in the Seldon Server")
    }
    if (parser.parse(args)){
      c = updateConf(c) // update from zookeeper args
      parser.parse(args) // overrride with args that were on command line

      val conf = new SparkConf().setAppName("Mahout")

      if (c.local)
        conf.setMaster("local")
          .set("spark.executor.memory", "8g")

      val sc = new SparkContext(conf)
      val spark = new SQLContext(sc)
      try
      {
        sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        if (c.awsKey.nonEmpty && c.awsSecret.nonEmpty)
        {
          sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", c.awsKey)
          sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", c.awsSecret)
        }
        println(c)
        /*val mf = new MfModelCreation(sc,c)
        mf.run()*/
        val config = c
        val inputFilesLocation = config.inputPath + "/" + config.client + "/actions/"
        val outputFilesLocation = config.outputPath + "/" + config.client +"/mahout/"
        val startTime = System.currentTimeMillis()
        val dataFile = inputFilesLocation + SparkUtils.getS3UnixGlob(config.startDay, config.days) + "/*"

        val ratings = prepareRatings(dataFile, config, sc)

        import spark.implicits._
        ratings.toDF().repartition(1).write.mode(SaveMode.Overwrite).csv(outputFilesLocation) //.map(r => (r.user, r.item, r.rating))
        val file = new File(outputFilesLocation).listFiles.filter(_.getName.startsWith("part")).lift(0).get
        println("file: "+file)
        file.renameTo(new File(outputFilesLocation+"/model.csv"))

        if (config.activate){
          val curator = new ZkCuratorHandler(config.zkHosts)
          if(curator.getCurator.getZookeeperClient.blockUntilConnectedOrTimedOut())
          {
            val zkPath = "/all_clients/"+config.client+"/mahout"
            val ensurePath = new EnsurePath(zkPath)
            ensurePath.ensure(curator.getCurator.getZookeeperClient)
            curator.getCurator.setData().forPath(zkPath,(outputFilesLocation).getBytes())
          }
        }
      }
      finally
      {
        println("Shutting down job")
        sc.stop()
      }
    }
    /*val config = Mahout.updateConf(c)*/

    //prepareSplits(percentage, preparedFile, folder, modelPath)


  }

}