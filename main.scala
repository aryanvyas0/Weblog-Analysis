// Databricks notebook source
// DBTITLE 1,Case Class for Weblog
// MAGIC %scala 
// MAGIC // For implicit conversions from RDDs to DataFrames
// MAGIC import spark.implicits._
// MAGIC import java.sql.Timestamp
// MAGIC 
// MAGIC case class weblog(
// MAGIC   Timestamp:String,                       ReportType:String,	
// MAGIC   Target:String,	                      Referrer:String,	
// MAGIC   Link:String,	                          SessionId:String,	
// MAGIC   SessionCount:Long,                      PageTitle:String,	
// MAGIC   LoadTime:String,	                      ViewTime:String,	
// MAGIC   Embedded:String,	                      Cookie:String,	
// MAGIC   HSResponseTime:String,	              PrefetchElement:String,	
// MAGIC   ElementsinHints:String,	              HintAlreadySeen:String, 
// MAGIC   Viewedfor1sttimePrefetched:String,	  Viewed1sttimenotPrefetched:String,	
// MAGIC   ConxSpeed:String,	                      ConxType:String,	
// MAGIC   PrevConxType:String,	                  VisitstoOrder:String,	
// MAGIC   DaystoOrder:String,	                  VisitFreq:String,	
// MAGIC   PurchaseFreq:String,	                  VisChip:String,	
// MAGIC   TimeinSession:String,	                  PreprocRules:String,	
// MAGIC   Secondssincelastpage:String, 	          ScreenResolution:String,	
// MAGIC   ColorDepth:String,	                  CookiesEnabled:String,	
// MAGIC   ReferringURL:String,	                  Product1stVisit:String,	
// MAGIC   FlashVersion:String,	                  UserAgent:String,	
// MAGIC   RemoteIP:String,	                      Serial:String,	
// MAGIC   TargetMatches:String,	                  NormalizedTarget:String,	
// MAGIC   ThirdPartyCookieEnabled:String )

// COMMAND ----------

// DBTITLE 1,Create an RDD of weblog objects from a text file, convert it to a Dataframe
// Create an RDD of weblog objects from a text file, convert it to a Dataframe
val weblogDF = spark.sparkContext
  .textFile("/FileStore/tables/Data.txt")
  .map(_.split("}"))
  .map(attributes => weblog(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5), 
                            attributes(6).toInt, attributes(7), attributes(8), attributes(9), attributes(10), 
                            attributes(11), attributes(12), attributes(13), attributes(14), attributes(15), 
                            attributes(16), attributes(17), attributes(18), attributes(19), attributes(20), 
                            attributes(21), attributes(22), attributes(23), attributes(24), attributes(25), 
                            attributes(26), attributes(27), attributes(28), attributes(29), attributes(30), 
                            attributes(31), attributes(32), attributes(33), attributes(34), attributes(35), 
                            attributes(36), attributes(37), attributes(38), attributes(39), attributes(40))).toDF()


// COMMAND ----------

// DBTITLE 1,Register the DataFrame as a Temporary view
// Register the DataFrame as a temporary view
weblogDF.createOrReplaceTempView("weblog")

display(weblogDF)


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(*) from weblog

// COMMAND ----------

// DBTITLE 1,Session Report
// MAGIC %md
// MAGIC 
// MAGIC * The number of sessions of time within the selected time frame 
// MAGIC * Helpful for web traffic.

// COMMAND ----------

// DBTITLE 1,Session's Report
// MAGIC %sql
// MAGIC 
// MAGIC select from_unixtime(Timestamp, "yyyy-MM-dd") as Date, from_unixtime(Timestamp, "HH") as Time, sum(SessionCount) as SessionCountByHour, 
// MAGIC (sum(SessionCount)/(select sum(SessionCount) from weblog) * 100) as Percentage  from weblog group by Timestamp order by Time;

// COMMAND ----------

// DBTITLE 1,Page Views Report
// MAGIC %md
// MAGIC 
// MAGIC * The number of pages that were viewed within the selected time frame.

// COMMAND ----------

// DBTITLE 1,Page Views Report
// MAGIC %sql
// MAGIC 
// MAGIC select from_unixtime(Timestamp, "yyyy-MM-dd") as Date, from_unixtime(Timestamp, "HH") as Time, count(PageTitle) as PagesViews, (count(PageTitle)/(select count(*) from weblog) * 100) as Percentage from  weblog group by Timestamp order by Time; 

// COMMAND ----------

// DBTITLE 1,New Visitor Report
// MAGIC %md
// MAGIC 
// MAGIC * New Visitors are the number of distinct new users that have visited the website during a given time period.

// COMMAND ----------

// DBTITLE 1,New Visitor Report
// MAGIC %sql
// MAGIC 
// MAGIC select from_unixtime(Timestamp, "yyyy-MM-dd") as Date, from_unixtime(Timestamp, "HH") as Time, count(Viewedfor1sttimePrefetched) as NewVisitor from weblog 
// MAGIC where Viewedfor1sttimePrefetched = "YES" group by Timestamp order by Timestamp

// COMMAND ----------

// DBTITLE 1,Referring Domains Report
// MAGIC %md
// MAGIC 
// MAGIC * Referring domains are web sites (social media) that end users visited before going to our web site. They can indicate popular links to our Website.

// COMMAND ----------

// DBTITLE 1,Refering Domains Report
// MAGIC %sql
// MAGIC 
// MAGIC SELECT Referrer, sum(SessionCount) as Session, count(Referrer) as Orders, sum(split(NormalizedTarget, '/')[1]) as Revenue from weblog group by Referrer;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select split(NormalizedTarget, '/')[0], split(NormalizedTarget, '/')[1] from weblog

// COMMAND ----------

// MAGIC %md
// MAGIC * The above table shows that the data was extracted from an array.
// MAGIC * [url, revenue]

// COMMAND ----------

// DBTITLE 1,Target Domains Report
// MAGIC %md
// MAGIC 
// MAGIC * Target domains are web sites that are used by the end users to buy the particular product.

// COMMAND ----------

// DBTITLE 1,Target Domains Report
// MAGIC %sql
// MAGIC 
// MAGIC SELECT Target, sum(SessionCount) as Session, count(Target) as Orders, sum(split(NormalizedTarget, '/')[1]) as Revenue from weblog group by Target;

// COMMAND ----------

// DBTITLE 1,Referring URL Report
// MAGIC %md
// MAGIC 
// MAGIC * This report is similar to the Referring Domains Report. This displays most popular reffering URL's

// COMMAND ----------

// DBTITLE 1,Referring URL Report
// MAGIC %sql
// MAGIC 
// MAGIC SELECT ReferringURL, sum(SessionCount) as Session, count(ReferringURL) as Orders, sum(split(NormalizedTarget, '/')[1]) as Revenue from weblog group by ReferringURL;

// COMMAND ----------

// DBTITLE 1,Top IP Addresses Report
// MAGIC %md
// MAGIC 
// MAGIC * This report ranks the IP addresses of visitors accessing our website in terms of number of sessions

// COMMAND ----------

// DBTITLE 1,Top IP Addresses Report
// MAGIC %sql
// MAGIC 
// MAGIC SELECT RemoteIP, sum(SessionCount) as Session, count(RemoteIP) as Orders, sum(split(NormalizedTarget, '/')[1]) as Revenue from weblog group by RemoteIP;

// COMMAND ----------

// DBTITLE 1,Search Query Report
// MAGIC %md
// MAGIC 
// MAGIC * Search queries are the key words entered into Internet search engines that provided results directing end users to our Web site. 
// MAGIC * This report depicts the top search queries that led users to our site and allows us to compare the number of page hits received by each search query.

// COMMAND ----------

// DBTITLE 1,Search Query Report
// MAGIC %sql
// MAGIC 
// MAGIC select split(PreprocRules,'=')[1] as Query, count(split(PreprocRules,'=')[1]) as Request from weblog group by Query order by Request desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select PreprocRules from weblog;

// COMMAND ----------

// DBTITLE 1, Cellular Network Technology
// MAGIC %sql
// MAGIC 
// MAGIC select count(ConxSpeed), ConxSpeed as  Cellular_Network_Technology from weblog group by ConxSpeed;

// COMMAND ----------

// DBTITLE 1,Mobile Connection Type
// MAGIC %sql
// MAGIC 
// MAGIC select ConxType, count(ConxType) from weblog group by ConxType;

// COMMAND ----------

// DBTITLE 1,Payment Type
// MAGIC %sql 
// MAGIC 
// MAGIC select VisChip, count(VisChip) from Weblog group by VisChip;

// COMMAND ----------

// DBTITLE 1,Device Screen Resolution
// MAGIC %sql
// MAGIC 
// MAGIC select ScreenResolution, count(ScreenResolution) from weblog group by ScreenResolution;

// COMMAND ----------

// DBTITLE 1,Browser Used for Shopping
// MAGIC %sql
// MAGIC 
// MAGIC select UserAgent as Browser, count(UserAgent)  from weblog group by UserAgent;

// COMMAND ----------

// DBTITLE 1,Device Type
// MAGIC %sql
// MAGIC 
// MAGIC select Serial as DeviceType, count(Serial) as countofSerial from weblog group by Serial order by countofSerial desc;
