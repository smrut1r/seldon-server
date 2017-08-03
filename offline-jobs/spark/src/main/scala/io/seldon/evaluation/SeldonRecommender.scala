package io.seldon.evaluation

import java.lang.{Float, Long}
import java.util

import com.typesafe.config.ConfigFactory
import io.seldon.api.state.{ClientAlgorithmStore, ZkCuratorHandler}
import io.seldon.api.state.zk.ZkClientConfigHandler
import io.seldon.db.jdo.JDOFactory
import io.seldon.memcache.SecurityHashPeer
import io.seldon.recommendation.{RecommendationPeer}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.springframework.context.support.ClassPathXmlApplicationContext

import scala.collection.JavaConverters._

case class Env(peer: RecommendationPeer, client: String, path: String)
case class RecRequest(env: Env, algos: util.List[String], user: Long, items: util.Set[Long], count: Int)

object SeldonRecommender {

  val client = "ahalife"
  val REC_CNTS = 50
  val schema = StructType(Seq(StructField("userId", LongType, false), StructField("itemId", LongType, false), StructField("preference", DoubleType, false)))

  def getRecommendationPeer():RecommendationPeer = {
    val ctx = new ClassPathXmlApplicationContext("classpath:api-service-ctx.xml")

    val appConfig = ConfigFactory.parseResourcesAnySyntax("env")
      .withFallback(ConfigFactory.parseResourcesAnySyntax("application"))
    val config = ConfigFactory.load(appConfig)

//    Try(Await.ready(Promise().future, Duration.create(5, "seconds")))

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

      val peer = ctx.getBean("recommendationPeer").asInstanceOf[RecommendationPeer]
      return peer
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        return null
    } finally {
      if (jdoFactory != null) jdoFactory.cleanupPM
    }
  }

  def recommend(testData: DataFrame, algos: util.List[String], count:Int, spark: SparkSession):DataFrame = {
    val peer = getRecommendationPeer()

    try {
      import spark.implicits._
      var users = testData.select("userId").as[Long].collect.toSet //map(r => r.getLong(0)).collect //as[Long].collect()
      //var items = testData.select("item").distinct().as[Long].collect.toSet.asJava //.map(r => r.getString(0)).collect().toSet[Long].asJava
      val len = users.size

      //algos.asScala.foreach(algo => {
      var cnt = 0f
      var recs = spark.emptyDataset[(Long, Long, Float)]
      /*val pb = new ProgressBar(algo, len)
      pb.start*/
      users.par.map(user => {
        try {
          var items = testData.select("itemId").where($"userId" === user).as[Long].collect.toSet.asJava
          //val recRequest = RecRequest(env, algoz, user, items, REC_CNTS)
          val recResults, _ = peer.getRecommendations(user, client, null, null, null, count, null, null, null, null, algos, items).getLeft
          val rec = recResults.getRecs()
          //val rec = recommend(recRequest)
          if(!rec.isEmpty)
            recs = recs.union(rec.asScala.filter(x => !Float.isNaN(x.getPrediction.toFloat)).map(x => (new Long(user), new Long(x.getContent), new Float(x.getPrediction))).toDS())
          cnt+=1
          var pct = ((cnt / len) * 100).toDouble
          pct = pct - (pct % 0.01)
          if (pct % 2 == 0) println(s"#### ${pct}% :(${cnt} of ${len}) of ${algos} completed")
          //pb.step
        } catch {
          case e: Exception => e.printStackTrace
        }
      })
      //pb.stop
      //val file = new File(path + fileName)
      return recs.toDF()
      //})
    } catch {
      case e: Exception =>
        e.printStackTrace
        return null
    }
  }

  def main(args: Array[String]) {
    //algos.add("RECENT_MATRIX_FACTOR")
    //algos.add("RECENT_SIMILAR_ITEMS")
    //algos.add("RECENT_TOPIC_MODEL")
    //algos.add("WORD2VEC")
    val algo = "RECENT_MATRIX_FACTOR"
    //val algo = "USER_BASED"

    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    val testData = spark.read.schema(schema).csv("/seldon-data/seldon-models/" + client +"/evaluation/"+ algo +"/test/")
    val recs = recommend(testData, util.Arrays.asList(algo), REC_CNTS, spark)
    recs.repartition(1).write.mode(SaveMode.Overwrite).csv("/seldon-data/seldon-models/" + client +"/evaluation/"+ algo +"/pred/")
    //recommend(modelPath, recPath)
    println("completed!!")
  }

}
