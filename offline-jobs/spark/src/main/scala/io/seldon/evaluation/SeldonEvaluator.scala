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
import net.recommenders.rival.evaluation.metric.error.{MAE, RMSE}
import net.recommenders.rival.evaluation.metric.ranking.{MAP, NDCG, PopularityStratifiedRecall, Precision, Recall}
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
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationListener
import org.springframework.context.event.ContextRefreshedEvent

import scala.io.Source
import scala.util.Try
//import scalikejdbc._
//import scalikejdbc.config._


object SeldonEvaluator {
  val PERCENTAGE: Float = 0.8f
  val NEIGH_SIZE: Int = 50
  val ATS = Array[Int](1, 3, 5, 10, 20, 30, 50)
  val REL_TH: Double = 3.0
  val SEED: Long = 2048L

  val client = "ahalife";
  val folder = "/seldon-data/seldon-models/ahalife"
  val modelPath = folder+"/model/"
  val recPath = folder+"/recommendations/"
  val dataFile = "/seldon-data/seldon-models/"+ client +"/actions/"+SparkUtils.getS3UnixGlob(1,180)+"/*"
  val preparedFile = folder+"/input.json"
  val percentage = PERCENTAGE

  val appConfig = ConfigFactory.parseResourcesAnySyntax("env").withFallback(ConfigFactory.parseResourcesAnySyntax("application"))
  val config = ConfigFactory.load(appConfig)

  val algos = Array("RECENT_MATRIX_FACTOR", "RECENT_SIMILAR_ITEMS", "RECENT_TOPIC_MODEL", "WORD2VEC")

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
  val conf = new SparkConf().setAppName("Seldon Validation").setMaster("local[2]")
    .set("spark.driver.memory", "30g")
    .set("spark.executor.memory", "30g")
    .set("spark.driver.maxResultSize", "10g")
  val sc = new SparkContext(conf)
  //val spark = new SQLContext(sc)
  val spark = SparkSession.builder().getOrCreate()
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

    evaluate(modelPath, recPath)
    println("completed!!")
    sc.stop()
  }

  def evaluate(splitPath: String, recPath: String) {
    var ndcgRes: Double = 0.0
    var precisionRes: Double = 0.0
    var rmseRes: Double = 0.0
    val i: Int = 0

    val testschema = StructType(Seq(StructField("user", LongType, false), StructField("item", LongType, false), StructField("preference", DoubleType, false), StructField("timestamp", LongType, true)))
    val tests = spark.read.option("delimiter", "\t").option("header","false").schema(testschema).csv(splitPath + "test_" + i + ".csv")
    val recschema = StructType(Seq(StructField("user", LongType, false), StructField("item", LongType, false), StructField("preference", DoubleType, false), StructField("timestamp", LongType, true)))
    val recs = spark.read.option("delimiter", "\t").option("header","false").schema(recschema).csv(recPath + "recs_" + i + ".csv")


    val testModel: DataModel[Long, Long] = loadDataModel(splitPath + "test_" + i + ".csv")

    /*val testFile: File = new File(splitPath + "test_" + i + ".csv")
    val recFile: File = new File(recPath + "recs_" + i + ".csv")
    var testModel: DataModel[Long, Long] = null
    var recModel: DataModel[Long, Long] = null
    try {
      testModel = new SimpleParser().parseData(testFile)
      recModel = new SimpleParser().parseData(recFile)
    }
    catch {
      case e: IOException => {
        e.printStackTrace
      }
    }*/

    algos.foreach(algo =>{
      val recModel: DataModel[Long, Long] = loadDataModel(recPath + algo + ".csv")

      val mae: MAE[Long, Long] = new MAE[Long, Long](recModel, testModel)
      mae.compute

      val rmse: RMSE[Long, Long] = new RMSE[Long, Long](recModel, testModel)
      rmse.compute

      val ndcg: NDCG[Long, Long] = new NDCG[Long, Long](recModel, testModel, ATS)
      ndcg.compute

      val precision: Precision[Long, Long] = new Precision[Long, Long](recModel, testModel, REL_TH, ATS)
      precision.compute

      val map: MAP[Long, Long] = new MAP[Long, Long](recModel, testModel, REL_TH, ATS)
      map.compute

      val recall: Recall[Long, Long] = new Recall[Long, Long](recModel, testModel, REL_TH, ATS)
      recall.compute

      /*val pStRecall: PopularityStratifiedRecall[Long, Long] = new PopularityStratifiedRecall[Long, Long](recModel, testModel, REL_TH, Array[Int](AT5,AT10,AT20))
      pStRecall.compute*/
      println("-------------------------")
      System.out.println(algo+"- MAE: " + mae.getValue)
      System.out.println(algo+"- RMSE: " + rmse.getValue)


      ATS.foreach(AT =>{
        println("-------------------------")
        System.out.println(algo+"- NDCG@" + AT + ": " + ndcg.getValueAt(AT))
        System.out.println(algo+"- Precision@" + AT + ": " + precision.getValueAt(AT))
        System.out.println(algo+"- MAP@" + AT + ": " + map.getValueAt(AT))
        System.out.println(algo+"- Recall@" + AT + ": " + recall.getValueAt(AT))
      })
    })

  }


  def loadDataModel(filePath: String): DataModel[Long, Long] = {
    val dataModel = new DataModel[Long, Long]()
    val dataSource = Source.fromFile(filePath)
    val len = 10000
    var cnt = 0f
    for (line <- dataSource.getLines) {
      val cols = line.split("\t").map(_.trim)

      dataModel.addPreference(cols(0).toLong, cols(1).toLong, cols(2).toDouble)
      if (cols.size>3) {
        dataModel.addTimestamp(cols(0).toLong, cols(1).toLong, cols(3).toLong)
      }
      cnt += 1f
      val pct = (cnt / len) * 100
      //if (pct % 2 == 0) println(s"#### filePath ${pct}% :(${cnt} of ${len}) completed")
    }
    dataSource.close
    dataModel
  }

  def loadH2ODataModel(filePath: String): DataModel[Long, Long] = {
    val dataModel = new DataModel[Long, Long]

    /*val h2oContext = H2OContext.getOrCreate(sc)
    val hdf: H2OFrame = new H2OFrame()(new File(filePath))

    val rdd = h2oContext.asRDD(hdf)
    val df = h2oContext.asDataFrame(hdf)(spark)

    val len = 10000
    var cnt = 0f
    df.map(row => {
      val userId = row.getAs[Long]("user")
      val itemId = row.getAs[Long]("item")
      val preference = row.getAs[Double]("preference")
      val timestamp = row.getAs[Long]("timestamp")
      dataModel.addPreference(userId, itemId, preference)
      if (timestamp != -1) {
        dataModel.addTimestamp(userId, itemId, timestamp)
      }
      cnt += 1f
      val pct = (cnt/len)*100
      if(pct%2 ==0) println(s"#### test ${pct}% :(${cnt} of ${len}) completed")
    })*/
    dataModel
  }

  def loadSparkDataModel(filePath: String): DataModel[Long, Long] = {
    val schema = StructType(Seq(StructField("user", LongType, false), StructField("item", LongType, false), StructField("preference", DoubleType, false), StructField("timestamp", LongType, true)))
    val dataModel = new DataModel[Long, Long]
    val test = spark.read.option("delimiter", "\t").option("header","false").schema(schema).csv(filePath)
    //val peopleSchemaRDD = spark.applySchema(test.rdd, schema)
    test.createTempView("test")
    val df = spark.sql("SELECT user, item, preference, timestamp FROM test")
    df.cache()

    val len = 1000 //testdf.count()
    var x = 0f
    df.map(row => {
      val userId = row.getAs[Long]("user")
      val itemId = row.getAs[Long]("item")
      val preference = row.getAs[Double]("preference")
      val timestamp = row.getAs[Long]("timestamp")
      dataModel.addPreference(userId, itemId, preference)
      if (timestamp != -1) {
        dataModel.addTimestamp(userId, itemId, timestamp)
      }
      x += 1f
      val pct = (x/len)*100
      if(pct%2 ==0) println(s"#### test ${pct}% :(${x} of ${len}) completed")
      dataModel
    }).collect().last
  }
}

//@RunWith(classOf[SpringJUnit4ClassRunner])
//@ContextConfiguration(locations = Array("classpath:/WEB-INF/spring/appServlet/api-service-ctx.xml"))
final class SeldonEvaluator private extends ApplicationListener[ContextRefreshedEvent] {
  override def onApplicationEvent(e: ContextRefreshedEvent): Unit = {
    val ctx = e.getApplicationContext();
    println("Context initialized...........")
  }
}
