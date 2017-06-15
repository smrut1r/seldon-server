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
package io.seldon.evaluation

import net.recommenders.rival.core.DataModel
import net.recommenders.rival.core.DataModelUtils
import net.recommenders.rival.core.Parser
import net.recommenders.rival.core.SimpleParser
import net.recommenders.rival.evaluation.metric.error.RMSE
import net.recommenders.rival.evaluation.metric.ranking.NDCG
import net.recommenders.rival.evaluation.metric.ranking.Precision
import net.recommenders.rival.evaluation.strategy.EvaluationStrategy
import net.recommenders.rival.recommend.frameworks.RecommenderIO
import net.recommenders.rival.recommend.frameworks.exceptions.RecommenderException
import net.recommenders.rival.recommend.frameworks.mahout.GenericRecommenderBuilder
import net.recommenders.rival.split.parser.MovielensParser
import net.recommenders.rival.split.splitter.RandomSplitter
import org.apache.mahout.cf.taste.common.TasteException
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel
import org.apache.mahout.cf.taste.recommender.RecommendedItem
import org.apache.mahout.cf.taste.recommender.Recommender
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.UnsupportedEncodingException
import java.lang.reflect.InvocationTargetException
import java.util.List
import java.lang.Long
import java.sql.{DriverManager, ResultSet}
import java.util
import java.util.concurrent.TimeUnit
import javax.servlet.ServletContextEvent

import akka.actor.{ActorRef, ActorSystem, Inbox, Props}
import com.typesafe.config.ConfigFactory
import io.seldon.spark.SparkUtils
import io.seldon.spark.mllib.{MfConfig, MfModelCreation}
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationListener
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.springframework.web.context.WebApplicationContext
import org.springframework.web.context.support.WebApplicationContextUtils

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.io.Source
import scala.util.Try
//import scalikejdbc._
//import scalikejdbc.config._

object SeldonDataSplitter {
  val PERCENTAGE: Float = 0.8f
  val NEIGH_SIZE: Int = 50
  val AT: Int = 10
  val REL_TH: Double = 3.0
  val SEED: Long = 2048L

  val client = "ahalife";
  val folder = "/seldon-data/seldon-models/ahalife"
  val modelPath = folder + "/model/"
  val recPath = folder + "/recommendations/"
  val dataFile = "/seldon-data/seldon-models/" + client + "/actions/" + SparkUtils.getS3UnixGlob(1, 180) + "/*"
  val preparedFile = folder + "/input.json"
  val percentage = PERCENTAGE


  /*val ctx = new ClassPathXmlApplicationContext("classpath:api-service-ctx.xml")
  val peer = ctx.getBean("recommendationPeer").asInstanceOf[RecommendationPeer]*/

  val appConfig = ConfigFactory.parseResourcesAnySyntax("env").withFallback(ConfigFactory.parseResourcesAnySyntax("application"))
  val config = ConfigFactory.load(appConfig)
  //val config = ConfigFactory.load("application.conf")

  // DBs.setup/DBs.setupAll loads specified JDBC driver classes.
  /*DBs.setupAll()
  case class Item(id: Long, name: String, value: String);
  val items = DB readOnly { implicit session =>
    sql"select i.client_item_id, ia.name, imv.value from items i, item_attr ia, item_map_varchar imv where i.item_id = imv.item_id and imv.attr_id = ia.attr_id".map(rs => Item(rs).list.apply()
  }
  DBs.closeAll()*/

  //import org.apache.spark.sql.SparkSession
  /*val spark = SparkSession.builder()
    .appName("Seldon Validation")
    .master("local[2]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()*/

  //Class.forName("com.mysql.jdbc.Driver").newInstance
//  val conf = new SparkConf().setAppName("Seldon Validation").setMaster("local[2]")
//    .set("spark.driver.memory", "12g")
//    .set("spark.executor.memory", "12g")
//    .set("spark.driver.maxResultSize", "4g")
  //val sc = new SparkContext(conf)
  //val spark = new SQLContext(sc)

  val spark = SparkSession
    .builder()
    .appName("Seldon Validation")
    //.config("spark.sql.warehouse.dir", warehouseLocation)
    .config("spark.driver.memory", "12g")
    .config("spark.executor.memory", "12g")
    .config("spark.driver.maxResultSize", "4g")
    .enableHiveSupport()
    .getOrCreate()
  import spark.implicits._

  /*val options = Map("driver" -> MYSQL_DRIVER,
      "url" -> MYSQL_CONNECTION_URL,
      "dbtable" -> SQL,
      "lowerBound" -> "0",
      "upperBound" -> "999999999",
      "partitionColumn" -> "emp_no",
      "numPartitions" -> "10"
    );
  val sqlDF = spark.load("jdbc", options);*/

  /*val sqlDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://10.0.0.29/ahalife")
    .option("user", "root")
    .option("password", "mypass")
    .option("dbtable", "items")
    .load()*/

  /*val itemRdd = new JdbcRDD(
    sc,
    () => {
      Class.forName(config.getString("db.default.driver"))
      DriverManager.getConnection(config.getString("db.default.url"))
    },
    config.getString("db.items"),
    0, 999999999, 1,
    (row : ResultSet) => (row.getInt("item_id"), row.getString("name"), row.getString("value"))
  )
  itemRdd.toDF().registerTempTable("items")
  val itemDF = spark.sql("select * from items")
  itemDF.show()*/

  def main(args: Array[String]) {

    /*spark.read.json(dataFile).createOrReplaceTempView("actions")
    val actionData = spark.sql("SELECT userid, itemid, type, timestamp_utc FROM actions")
    actionData.repartition(1).write.mode(SaveMode.Overwrite).json(preparedFile)*/

    var c = MfConfig()
    val parser = new scopt.OptionParser[Unit]("SeldonDataSplitter") {
      opt[String]('c', "client") required() valueName("<client>") foreach { x => c = c.copy(client = x) } text("client name (will be used as db and folder suffix)")
      opt[String]('z', "zookeeper") valueName("zookeeper hosts") foreach { x => c = c.copy(zkHosts = x) } text("zookeeper hosts (comma separated)")
    }
    parser.parse(args)
    val config = MfModelCreation.updateConf(c)
    val ratings = MfModelCreation.prepareRatings(dataFile, config, spark.sparkContext)

    ratings.toDF().repartition(1).write.mode(SaveMode.Overwrite).json(preparedFile) //.map(r => (r.user, r.item, r.rating))

    prepareSplits(percentage, preparedFile, folder, modelPath)

    spark.stop()
  }

  def prepareSplits(percentage: Float, inFile: String, folder: String, outPath: String) {
    val perUser: Boolean = true
    val perItem: Boolean = false
    val seed: Long = SEED
    val parser = new SeldonEventParser
    val data = parser.parseData(new File(inFile))

    val splits: Array[DataModel[Long, Long]] = new RandomSplitter(percentage, perUser, seed, perItem).split(data)
    val dir: File = new File(outPath)
    if (!dir.exists) {
      if (!dir.mkdir) {
        System.err.println("Directory " + dir + " could not be created")
        return
      }
    }
    var i: Int = 0
    while (i < splits.length / 2) {
      {
        val training: DataModel[Long, Long] = splits(2 * i)
        val test: DataModel[Long, Long] = splits(2 * i + 1)
        val trainingFile: String = outPath + "train_" + i + ".csv"
        val testFile: String = outPath + "test_" + i + ".csv"
        //System.out.println("train: " + trainingFile)
        //System.out.println("test: " + testFile)
        val overwrite: Boolean = true
        try {
          DataModelUtils.saveDataModel(training, trainingFile, overwrite)
          DataModelUtils.saveDataModel(test, testFile, overwrite)
        } catch {
          case e: Exception => e.printStackTrace
        }
      }

      i += 1;
    }
  }

}
//@RunWith(classOf[SpringJUnit4ClassRunner])
//@ContextConfiguration(locations = Array("classpath:/WEB-INF/spring/appServlet/api-service-ctx.xml"))
final class SeldonDataSplitter private extends ApplicationListener[ContextRefreshedEvent] {
  override def onApplicationEvent(e: ContextRefreshedEvent): Unit = {
    val ctx = e.getApplicationContext();
    println("Context initialized...........")
  }
}
