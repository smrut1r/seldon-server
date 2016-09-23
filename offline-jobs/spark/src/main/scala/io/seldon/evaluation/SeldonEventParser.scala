package io.seldon.evaluation

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//
import net.recommenders.rival.core.DataModel
import net.recommenders.rival.core.Parser
import net.recommenders.rival.core.SimpleParser
import java.io.BufferedReader
import java.io.File
import java.io.IOException
import java.io.BufferedReader
import java.io.File
import java.io.IOException
import java.lang.Long

import net.recommenders.rival.core.DataModel
import net.recommenders.rival.core.Parser
import net.recommenders.rival.core.SimpleParser
import rapture.json.Json

import scala.io.Source

object SeldonEventParser {
  val USER_TOK: Int = 0
  val ITEM_TOK: Int = 1
  val RATING_TOK: Int = 2
  val TIME_TOK: Int = 3
}

class SeldonEventParser extends Parser[Long, Long] {

  @throws[IOException]
  def parseData(f: File): DataModel[Long, Long] = {
    val dataset = new DataModel[Long, Long]
    val file = f.listFiles.filter(_.getName.startsWith("part")).lift(0).get
    println("file: "+file)
    for(line <- Source.fromFile(file).getLines())
      parseLine(line, dataset)

    return dataset
  }

  private def parseLine(line: String, dataset: DataModel[Long, Long]) {
    //{"client": "ahalife", "client_item": "164942990", "client_user": "149000015630", "item": 149000015630, "rectag": "default", "timestamp_utc": "2016-01-02T13:38:19Z", "type": 2, "user": 467863, "value": 1.0}
    import rapture.json.jsonBackends.scalaJson._
    val src: Json = Json.parse(line)

    /* var toks: Array[String] = null
    if (line.contains("::")) {
      toks = line.split("::")
    }
    else {
      toks = line.split("\t")
    }*/
    //println(src)

    val user = src.user.toString().toLong
    val item = src.item.toString().toLong
    val timestamp = 0l //src.timestamp_utc.toString().toLong
    //val preference = src.`type`.toString().toDouble
    val rating = src.rating.toString().toDouble
    //println(user+" "+item+ ""+rating+ ""+timestamp)
    dataset.addPreference(user, item, rating)
    dataset.addTimestamp(user, item, timestamp)
  }
}