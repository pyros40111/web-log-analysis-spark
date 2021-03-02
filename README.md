# Web Log Analysis
There are codes in this repo to analyze logs of website request records. 

## Goals
1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
2. Determine the average session time
3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
4. Find the most engaged users, ie the IPs with the longest session times

## Idea
Based on goals above, it seems that we need to process log data to find out which session each request belongs to.
And we should assume that the amount of data is too large to use only one machine for ETL.
So Spark and HDFS are reasonable tools for this scenario.
And because we need to aggregate data by session, we'll use window function of Spark SQL.

## Environment
Below is details of my environment.
- Scala: 2.11.8
- Spark: 2.3.0
- Hadoop(only use HDFS): 2.7
- sbt: 1.4.7
- Java: AdoptOpenJDK 1.8.0_222

## Configurations
If you want to change some environment settings, there are some of them are configured. 
Just write your configurations to `src/main/resources/application.conf`

Example of `application.conf`:
```text
data {
  logFile = "hdfs://127.0.0.1:8020/data/web_log_sample.log"
  resultFolder = "hdfs://127.0.0.1:8020/results/"
}

spark {
  appName = "web-log-analysis"
  master = "spark://127.0.0.1:7077"
}

weblog {
  sessionExpireMilliseconds = 900000
}

```
- data.path: file path of web log file
- data.resultFolder: path of folder to place result files
- spark.appName: application name of this spark application
- spark.master: Address of your Spark Cluster
- weblog.sessionExpireMilliseconds: How long will the session expire

## Result
Result files are too large to be pushed to git remote repo.
So files will be written to HDFS.
Just print parts of results below. 

Goal01: 
```text
Sessionize the web log by IP
+--------------------+-------------------+--------------------+--------+
|           sessionId|           clientIp|                 url|duration|
+--------------------+-------------------+--------------------+--------+
|1.187.167.214:652...|1.187.167.214:65257|GET http://www.pa...|       0|
|1.187.170.77:64760_0| 1.187.170.77:64760|GET https://paytm...|       0|
|1.187.179.217:345...|1.187.179.217:34549|GET https://paytm...|       0|
|1.187.179.217:345...|1.187.179.217:34549|GET https://paytm...|  104677|
|1.187.179.217:345...|1.187.179.217:34549|GET https://paytm...|  265001|
+--------------------+-------------------+--------------------+--------+

```

Goal02: 
```text
Determine the average session time
+------------------+
|  avg(sessionTime)|
+------------------+
|1751570.1449229824|
+------------------+

```

Goal03: 
```text
Determine unique URL visits per session
+--------------------+----------+
|           sessionId|uniqueUrls|
+--------------------+----------+
|106.51.132.54:5048_0|       411|
|106.51.132.54:5049_0|       322|
|106.51.132.54:4508_0|       292|
|106.51.132.54:5037_0|       272|
|106.51.132.54:4974_0|       251|
+--------------------+----------+

```

Goal04: 
```text
Find the most engaged users, ie the IPs with the longest session times
+-------------------+----------------+
|           clientIp|totalSessionTime|
+-------------------+----------------+
|119.81.61.166:36205|        67413284|
| 52.74.219.71:55433|        67348576|
|106.186.23.95:42887|        67347959|
|119.81.61.166:36946|        67295731|
|119.81.61.166:34305|        67279915|
+-------------------+----------------+

```