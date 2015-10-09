import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.types.{StructType,StructField,StringType}

/*Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
  the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
  CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
  and limitations under the License.*/


/**Like CloudTrailToSQL, S3LogsToSQL is written for you to cut and paste it into your spark-shell and use it from there.
  * TODO: We do not auto-discover your S3 logs so you need to pass a bucket and prefix.
  *
  * Sample use:
  *  1. Start the Spark shell as follows:
  spark-shell  --master yarn-client  --num-executors 40  --conf spark.executor.cores=2
  *  2. Copy and paste this entire file into the shell.  It will create an object called S3LogsToSQL
  *  3. Load your S3 logs into a Hive table by running a command like the following:
  S3LogsToSQL.createHiveTable("<BUCKET>", "S3logs", sqlContext)
  *  4. Query them, e.g.,
  sqlContext.sql("select distinct ip from s3logshive").show(10000)
  * */
object S3LogsToSQL extends Logging {
  val TABLE_NAME = "s3logs"
  val schemaString = "bucketOwner bucket date ip requester operation key"
  val RDD_PARTITION_COUNT = 1000 //TODO: Consider making this dynamic and configurable.

  // Generate the schema from the schema string
  val schema =
    StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

  def rawS3Logs(sparkContext:SparkContext, logsBucket:String, logsPrefix:String):RDD[String] = {
    val path = "s3://" + logsBucket + "/" + logsPrefix + "*"
    val rawStrings:RDD[String] = sparkContext.textFile(path).coalesce(RDD_PARTITION_COUNT, true)
    rawStrings
  }

  def rawS3LogsToTempTable(rawStrings:RDD[String], sqlContext:SQLContext):DataFrame = {
    val s3LogsDF = rawS3LogsToDataFrame(rawStrings, sqlContext)
    s3LogsDF.registerTempTable(TABLE_NAME)
    s3LogsDF
  }

  def rawS3LogsToHiveTable(rawStrings:RDD[String], sqlContext:SQLContext):DataFrame = {
    val s3LogsDF = rawS3LogsToDataFrame(rawStrings, sqlContext)
    s3LogsDF.write.saveAsTable(TABLE_NAME+"hive")
    s3LogsDF
  }

  /** See http://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html for the format.
    * TODO, Right now we only load in part of the logs.  It would be better to use regexes or a more thoughtful
    * parsing scheme.*/
  def rawS3LogsToDataFrame(rawStrings:RDD[String], sqlContext:SQLContext):DataFrame = {
    val rowRDD = rawStrings.map((line:String) => {
      val lineArray = line.split(" ")

      if (lineArray.length >= 9) {
        val bucketOwner = lineArray(0)
        val bucket = lineArray(1)
        val date = (lineArray(2) + " " + lineArray(3))
        val ip = lineArray(4)
        val requester = lineArray(5)
        val operation = lineArray(7)
        val key = lineArray(8)
        Some(Row(bucketOwner, bucket, date, ip, requester, operation, key))
      }
      else {
        println("Could not parse the following line: " + line)
        None
      }
    }).filter(_.isDefined).map(_.get)

    val s3LogsDF = sqlContext.createDataFrame(rowRDD, schema)
    s3LogsDF.cache()
    s3LogsDF
  }

  def createTable(logsBucket:String, logsPrefix:String, sqlContext:SQLContext):DataFrame = {
    val rawLogsRDD = rawS3Logs(sqlContext.sparkContext, logsBucket, logsPrefix)
    rawS3LogsToTempTable(rawLogsRDD, sqlContext)
  }

  def createHiveTable(logsBucket:String, logsPrefix:String, sqlContext:SQLContext):DataFrame = {
    val rawLogsRDD = rawS3Logs(sqlContext.sparkContext, logsBucket, logsPrefix)
    rawS3LogsToHiveTable(rawLogsRDD, sqlContext)
  }
}

