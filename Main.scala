package com.tutorial.weblog

import java.util._
import java.time._
import java.util.concurrent.TimeUnit

import scala.util.control.Breaks._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.List

//case class WebLog(timestamp: String, elb: String, client_port: String,
//                  backend_port: String, request_processing_time: Float, backend_processing_time: Float, response_processing_time: Float,
//                  elb_status_code: Int, backend_status_code: Int, received_bytes: Int, sent_bytes: Int,
//                  request: String, user_agent: String, ssl_cipher: String, ssl_protocol: String){
//  val time = Instant.parse(timestamp)
//  val ip = client_port.split(":")(0)
//}

case class WebLog(timestamp: String, client_port: String, backend_port: String, url: String){
  val time = Instant.parse(timestamp)
  val ip = client_port.split(":")(0)
}

object Main {

  val regex = """(\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z)) (\w+-\w+) (\d+.\d+.\d+.\d+:.\d+) (\d+.\d+.\d+.\d+:.\d+) (\d+.\d+) (\d+.\d+) (\d+.\d+) (\d+) (\d+) (\d+) (\d+) "((?:[^"]|")+)" "((?:[^"]|")+)" (([\w-]+)|-) ((\w+.\d+)|-)""".r

  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("weblog-challenge").setMaster("local[*]")
    val sc = new SparkContext(conf)
//    val startDate = new Date()
//    println(startDate.toLocaleString)
    processChallenge(sc)
//    val endDate = new Date()
//    println(endDate.toLocaleString)
  }

  def processChallenge(sc: SparkContext): Unit ={
    val data = sc.textFile(s"C:\\2015_07_22_mktplace_shop_web_log_sample.log.gz")

    val results = data.map(log => {
      parseToWebLog(log)
    })
    .sortBy(t => t.time)  // Since we are working with RDD, results may be out of order. We must sort them by the timestamp
    println(s"collectedResults ${results.collect().length}")

    // All WebLogs per IP
    val webLogsByIp = results.map(wl => (wl.ip, List(wl))).reduceByKey((a, b) => a ++ b)
    println(s"Retrieved webLogsByIp: ${webLogsByIp.count()}")


    // **  #1: Sessionize the web log by ip   **
    val webLogSessionsByIp = webLogsByIp.map(sessionize)
    println(s"Retrieved webLogSessionsByIp * This answers #1 *")

    // DEBUG:
//    webLogSessionsByIp.take(10).foreach(log => {
//      println("******")
//      println(s"IP: ${log._1}")
//      log._2.foreach(ses => {
//        ses.foreach(println(_))
//        println("----")
//      })
//      println("******")
//      println("")
//    })

    // **  #2: Average session time for all requests   **
    val webLogSessionDurations = webLogSessionsByIp.map(wl => {
      val sessions = wl._2
      val durations = sessions.map(s => getDuration(s))
      (wl._1, durations)
    })
    val allDurations = webLogSessionDurations.flatMap(log => log._2)
    val totalDuration = allDurations.reduce((a,b) => a + b)
    val averageDuration: Long = totalDuration / allDurations.count()
    println(s"The Average duration is: ${TimeUnit.MILLISECONDS.toMinutes(averageDuration)} minutes or $averageDuration milliseconds")


    // ** #3: Unique URLs per session  **
    val webLogUniqueUrlsPerSessionByIp = webLogSessionsByIp.map(sessions => {
      val uniqueSessionsUrls = sessions._2.map(session => {
        session.map(s => s.url -> s).toSet.size
      })
      uniqueSessionsUrls
    })

    // ** #4:  Users with Longest Sessions **
    val usersWithLongestSessions = webLogSessionDurations.map(s => (s._1, s._2.max)).map(i => i.swap).sortByKey(false, 1).map(item => item.swap)
    usersWithLongestSessions.take(50).foreach(u => {
      println(s"ip: ${u._1} duration: ${u._2}")
    })
  }

  def getDuration(sessions: List[WebLog]): Long = {
    val last = sessions.map(s => s.time).max
    val first = sessions.map(s => s.time).min
    Math.abs(last.toEpochMilli - first.toEpochMilli)
  }

  def sessionize(webLog: (String, List[WebLog])): (String, List[List[WebLog]]) ={
    val sessionDurationInSeconds = 900
    val allLogs = webLog._2

    var sessions = List[List[WebLog]]()
    var currentSession = List[WebLog](allLogs.head)
    val flushSessions = () => {
      // currentSession.reverse to make sure that weblogs are ordered in ascending order (oldest log first)
      sessions = currentSession.reverse :: sessions
      currentSession = List[WebLog]()
    }

    for (iterator <- allLogs.indices){
      val currentTime = allLogs(iterator).time
      val nextIndex = iterator + 1
      if (nextIndex < allLogs.length) {
        val nextTime = allLogs(nextIndex).time
        if (nextTime.minusSeconds(sessionDurationInSeconds).isAfter(currentTime)) {
          // not same session
          flushSessions()
        }
        currentSession = allLogs(nextIndex) :: currentSession
      }
    }
    if (currentSession.nonEmpty){
      flushSessions()
    }

    // sessions.reverse to make sure sessions are ordered in ascending order (oldest sessions first)
    (webLog._1, sessions.reverse)
  }

//  def parseToWebLogRx(log: String): WebLog = {
//    val matchesOpt = regex.unapplySeq(log)
//    if (matchesOpt.isEmpty) return null;
//    val matches = matchesOpt.get
//    WebLog(matches(0), matches(2), "")
//  }

//  def parseToWebLog(log: String): WebLog = {
//    val split = log.split(" ")
//    WebLog(split(0), split(2), split(3))
//  }

  def parseToWebLog(log: String): WebLog = {
    val split = log.split(" ")
    val urlIndex = log.indexOf("http")
    val endIndex = log.indexOf(" HTTP/")
    // Note:  Alternatively, we could also choose to exclude query strings if the url endpoint can be considered unique
    // For this example, the entire url part is considered unique including the query strings
    val url = log.substring(urlIndex, endIndex)
    WebLog(split(0), split(2), split(3), url)
  }
}


/**

It is possible to identify unique users from same ip, if we observe the assigned load balancer for the request.

  */