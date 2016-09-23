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
import io.seldon.api.state.{ClientAlgorithmStore, ZkCuratorHandler}
import io.seldon.api.state.zk.ZkClientConfigHandler
import io.seldon.db.jdo.JDOFactory
import io.seldon.memcache.SecurityHashPeer
import io.seldon.recommendation.{RecommendationPeer, RecommendationResult}
import io.seldon.spark.SparkUtils
import org.apache.commons.collections.IteratorUtils
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{SQLContext, SaveMode}
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

case class Env(peer: RecommendationPeer, client: String, path: String)
case class RecRequest(env: Env, algos: util.List[String], user: Long, item: util.Set[Long], count: Int)

object SeldonRecommender {

  val ctx = new ClassPathXmlApplicationContext("classpath:api-service-ctx.xml")

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
  val conf = new SparkConf().setAppName("Seldon Validation").setMaster("local[4]")
    .set("spark.driver.memory", "12g")
    .set("spark.executor.memory", "12g")
    .set("spark.driver.maxResultSize", "4g")
  val sc = new SparkContext(conf)
  val spark = new SQLContext(sc)

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

    val system = ActorSystem("SeldonSystem")
    val zooRefresher = system.actorOf(Props[ZooRefresher], "ZooRefresher")
    //val inbox = Inbox.create(system)
    zooRefresher ! ctx

    //recommend(modelPath, recPath)
    println("completed!!")
    sc.stop()
  }

}

//@RunWith(classOf[SpringJUnit4ClassRunner])
//@ContextConfiguration(locations = Array("classpath:/WEB-INF/spring/appServlet/api-service-ctx.xml"))
final class SeldonRecommender private extends ApplicationListener[ContextRefreshedEvent] {
  override def onApplicationEvent(e: ContextRefreshedEvent): Unit = {
    val ctx = e.getApplicationContext();
    println("Context initialized...........")
  }
}

import akka.actor._

class ZooRefresher extends Actor {
  def receive = {
    case ctx: ClassPathXmlApplicationContext => contextInitialized(ctx)
    case _       => println("huh???")
  }

  def contextInitialized(ctx: ClassPathXmlApplicationContext) {
    println("Starting")
    Try(Await.ready(Promise().future, Duration.create(5, "seconds")))
    println("Done")

    var jdoFactory: JDOFactory = null
    try {
      ctx.getStartupDate
      jdoFactory = ctx.getBean("JDOFactory").asInstanceOf[JDOFactory]
      val zkClientConfigHandler = ctx.getBean("zkClientConfigHandler").asInstanceOf[ZkClientConfigHandler]
      SecurityHashPeer.initialise
      val curatorHandler = ZkCuratorHandler.getPeer

      zkClientConfigHandler.contextIntialised

      val clientAlgorithmStore = ctx.getBean("clientAlgorithmStore").asInstanceOf[ClientAlgorithmStore]
      val source = scala.io.Source.fromFile("/seldon-data/conf/zkroot/all_clients/ahalife/algs/_data_")
      val algs = try source.mkString finally source.close()
      clientAlgorithmStore.configUpdated("algs", algs)

      val system = ActorSystem("SeldonSystem")
      val recommender = system.actorOf(Props[Recommender], "Recommender")
      //val inbox = Inbox.create(system)
      recommender ! ctx
    }
    catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (jdoFactory != null) jdoFactory.cleanupPM
    }
  }
}

class Recommender extends Actor {
  val i: Int = 0
  val client = "ahalife"
  val folder = "/seldon-data/seldon-models/ahalife"
  val modelPath = folder+"/model/"
  val recPath = folder+"/recommendations/"
  val SCALE = 30

  def receive = {
    /*case RecRequest(Env(peer, algos, client, path, fileName), user, count) => {
      val recResults = peer.getRecommendations(user, client, null, 0, null, count, "1L", 0L, "", "", algos, null)
      val items = recResults.getRecs().asScala.map(x => new GenericRecommendedItem(x.getContent, x.getPrediction.toFloat)).asJava
      //sender ! items
      val file = new File(path+fileName)
      RecommenderIO.writeData(user, items, path, fileName, file.length() != 0, null)
    }*/
    case ctx: ClassPathXmlApplicationContext => recommend(ctx)
    case _       => println("huh???")
  }

  def getDataModel(x: String, inPath: String, outPath: String): FileDataModel = {
    try {
      x match {
        case "train" => new FileDataModel (new File (inPath + "train_" + 0 + ".csv") )
        case "test" => new FileDataModel (new File (inPath + "test_" + 0 + ".csv") )
        case _ =>  null
      }
    } catch {
      case e: IOException => {
        e.printStackTrace
        null
      }
    }
  }


  def recommend(ctx: ClassPathXmlApplicationContext) {
    val peer = ctx.getBean("recommendationPeer").asInstanceOf[RecommendationPeer]
    println("Starting2")
    Try(Await.ready(Promise().future, Duration.create(10, "seconds")))
    println("Done2")

    val algos = new util.ArrayList[String]()
    algos.add("RECENT_MATRIX_FACTOR")
    algos.add("RECENT_SIMILAR_ITEMS")
    algos.add("RECENT_TOPIC_MODEL")
    algos.add("WORD2VEC")

    val trainModel = getDataModel("train", modelPath, recPath)
    val testModel = getDataModel("test", modelPath, recPath)

    algos.asScala.foreach(algo =>{
      new File(recPath + algo + ".csv").delete()
    })
    val env = Env(peer, client, recPath)

    try {
      var users = testModel.getUserIDs.asScala.toList
      var items = testModel.getItemIDs.asScala.toList.toSet.asJava
      val len = 3000 //testModel.getUserIDs.asScala.length
      //val testModel2 = getDataModel("test", inPath, outPath)

      var x = 0f
      /*users.asScala.toStream.par.map(u => {
        val recRequest = RecRequestBkp(env, u, trainModel.getNumItems())
        recommend(recRequest)
        x += 1f
        val pct = (x/len)*100
        if(pct%2 ==0) println(s"#### ${pct}% :(${x} of ${len}) completed")
      })*/
      algos.asScala.foreach(algo => {
        val algoz = util.Arrays.asList(algo)
        for (u <- users) {
          //while (items.hasNext){
          //val u = users.nextLong
          //val i = items.toSet
          val recRequest = RecRequest(env, algoz, u, items, 1000) //trainModel.getNumItems()
          //recommender ! recRequest
          //inbox.send(recommender, recRequest)
          //val items: List[RecommendedItem] = inbox.receive(Duration.create(1, TimeUnit.SECONDS)).asInstanceOf
          recommend(recRequest)
          x += 1f
          val pct = (x / len) * 100
          if (pct % 2 == 0) println(s"#### ${pct}% :(${x} of ${len}) completed")
          //}
        }
      })
      println("Task completed!!!")
    } catch {
      case e: TasteException => e.printStackTrace
    }
  }

  def recommend(x: Any): Any = x match {
    case RecRequest(Env(peer, client, path), algos, user, scoreItems, count) => {
      try {
        val recResults = peer.getRecommendations(user, client, null, 0, null, count, "1L", 0L, "", "", algos, scoreItems)
        val items = recResults.getRecs().asScala.map(x => new GenericRecommendedItem(x.getContent, (SCALE * x.getPrediction).toFloat)).asJava
        //sender ! items
        val fileName = algos.toArray().mkString("+") + ".csv"
        val file = new File(path + fileName)
        RecommenderIO.writeData(user, items, path, fileName, file.length() != 0, null)
      }catch {
        case e:Exception => e.printStackTrace()
      }
    }
    case _       => println("huh???")
  }
}
