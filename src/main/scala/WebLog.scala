import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex
// spark
import org.apache.spark.SparkConf
// config
import utils.EnvConfig


object WebLog {
  // case class for schema of WebLog
  case class WebLog(time: String, clientIp: String, url: String, userAgent: String, epoch: Long)
  // case class for schema of WebLogRaw
  // raw data example: 2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
  case class WebLogRaw(time: String, elb: String, clientIp: String, backendIp:String ,request_processing_time: String, backend_processing_time: String
                       , response_processing_time: String, elb_status_code: String, backend_status_code: String, received_bytes: String
                       , sent_bytes: String, request: String, user_agent: String, ssl_cipher: String
                       , ssl_protocol: String)

  // convert ISO time format to timestamp, it would be easy to calculate duration between two requests
  def addEpoch(date:String): Long = {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val dt = df.parse(date)
    dt.getTime()
  }

  // regex pattern to parse each line of log
  val pattern: Regex = """^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r

  //parsing each record in log file using the function
  def parseLine(line: String): WebLogRaw = {
    try {
      val res = pattern.findFirstMatchIn(line)
      if(res.isEmpty) WebLogRaw("None", "-", "-", "", "",  "", "", "-", "-","","","","","","")
      else {
        val m = res.get
        if (m.group(4).equals("-")) {
          WebLogRaw(m.group(1), m.group(2), m.group(3),"",
            m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13)
            , m.group(14), m.group(15))
        }
        else {
          WebLogRaw(m.group(1), m.group(2), m.group(3),m.group(4),
            m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), m.group(12), m.group(13)
            , m.group(14), m.group(15))
        }
      }
    }
    catch {
      case e: Exception => WebLogRaw("None", "-", "-", "", "",  "", "", "-", "-","","","","","","")
    }
  }

  def main(args: Array[String]): Unit = {
    // load config
    EnvConfig.loadLocalConfig("conf/application.conf")
    val config = EnvConfig.getEnvConfig()
    val sparkAppName = config.SPARK_APP_NAME
    val sparkMaster = config.SPARK_MASTER
    val logFilePath = config.LOG_FILE_PATH
    val sessionExpireMilliseconds = config.SESSION_EXPIRE_MILLISECONDS
    // create SparkSession
    val sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    // import for spark sql
    import sparkSession.implicits._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val webLogRawRecords = sparkSession.sparkContext.textFile(logFilePath)

    val webLogParsedRecords = webLogRawRecords.map(parseLine)

    // ignore all lines that backendIp is empty, we assume that kind of request is invalid
    // then just select columns that we need
    // also add epoch converted from ISO time as new column
    val webLogFilteredRecords = webLogParsedRecords.filter(webLogRaw => webLogRaw.backendIp!="-")
      .map(raw => WebLog(raw.time, raw.clientIp, raw.request, raw.user_agent, addEpoch(raw.time)))

    // create dataframe using SparkSession because we need to use api of spark dataframe
    val webRecordsDf = sparkSession.createDataFrame(webLogFilteredRecords)

    // create window for df
    // which is partitioned by clientIp and ordered by epoch
    val window = Window.partitionBy("clientIp").orderBy("epoch")

    // add column prevEpoch which represent the duration from previous request to this one
    val webRecordsWithPrevEpochDf = webRecordsDf.withColumn("prevEpoch", lag("epoch", 1).over(window))

    // clean prevEpoch to replace null with epoch and create new column cleanPrevEpoch
    val webRecordsWithCleanEpochDf = webRecordsWithPrevEpochDf.withColumn("cleanPrevEpoch", coalesce($"prevEpoch", $"epoch"))

    // add new column duration
    // duration means the time from previous request to this one
    // duration = epoch - cleanPrevEpoch
    val webRecordsWithDurationDf = webRecordsWithCleanEpochDf.withColumn("duration", $"epoch" - $"cleanPrevEpoch")

    // create udf to calculate isNewSession value
    // 1 -> the first request of a new session
    // 0 -> not the first request of a new session
    val isNewSession = udf((duration: Long) => {
      if(duration > sessionExpireMilliseconds) 1
      else 0
    })
    val webRecordsWithIsNewSessionDf = webRecordsWithDurationDf.withColumn("isNewSession", isNewSession($"duration"))

    // add column sessionNum
    // group by window and sum isNewSession column
    // the number represent the number of session request belongs group by clientIp
    val webRecordsWithSessionNumDf = webRecordsWithIsNewSessionDf.withColumn("sessionNum",
      sum("isNewSession").over(window).cast("string"))

    // create udf to generate sessionId
    // sessionId = (clientIp)_(sessionNum)
    val generateSessionId = udf((clientIp: String, sessionNum: String) => {clientIp + "_" + sessionNum})

    // ans01: Sessionize the web log by IP
    // each row has its sessionId which represent which session it belongs to
    val webRecordsWithSessionIdDf = webRecordsWithSessionNumDf.withColumn("sessionId", generateSessionId($"clientIp", $"sessionNum"))
      .select($"sessionId", $"clientIp", $"url", $"duration").cache()
    webRecordsWithSessionIdDf.show(5)
    //webRecordsWithSessionIdDf.coalesce(1).write.format("csv").save("results/ans01")

    // ans02: Determine the average session time
    val sessionsWithTimeDf = webRecordsWithSessionIdDf.groupBy($"sessionId").sum("duration").withColumnRenamed("sum(duration)", "sessionTime")
    val sessionsMeanTimeDf = sessionsWithTimeDf.select(mean("sessionTime"))
    sessionsMeanTimeDf.show(5)
    //sessionsMeanTimeDf.coalesce(1).write.format("csv").save("results/ans02")

    // ans03: Determine unique URL visits per session
    val sessionsUniqueVisitDf = webRecordsWithSessionIdDf.groupBy($"sessionId").agg(collect_set($"url"))
      .select($"sessionId", size($"collect_set(url)")).withColumnRenamed("size(collect_set(url))", "uniqueUrls")
      .sort($"uniqueUrls".desc)
    sessionsUniqueVisitDf.show(5)
    //sessionsUniqueVisitDf.coalesce(1).write.format("csv").save("results/ans03")

    // ans04: Find the most engaged users
    val sessionsMostEngagedUserDf = webRecordsWithSessionIdDf.groupBy($"clientIp").sum("duration")
      .withColumnRenamed("sum(duration)", "totalSessionTime").sort($"totalSessionTime".desc)
    sessionsMostEngagedUserDf.show(5)
    //sessionsMostEngagedUserDf.coalesce(1).write.format("csv").save("results/ans04")

  }
}

