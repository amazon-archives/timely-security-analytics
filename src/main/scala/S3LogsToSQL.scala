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

object S3LogsToSQL extends Logging {
  val schemaString = "bucketOwner bucket date ip requester operation key"
  // Generate the schema based on the string of schema
  val schema =
    StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

  def rawS3Logs(sparkContext:SparkContext, logsBucket:String, logsPrefix:String):RDD[String] = {
    val path = "s3://" + logsBucket + "/" + logsPrefix + "*"
    val rawStrings:RDD[String] = sparkContext.textFile(path)
    rawStrings
  }

  /** See http://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html */

  def S3Logs(rawStrings:RDD[String], sqlContext:SQLContext):DataFrame = {
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
    s3LogsDF.registerTempTable("s3logs")
    s3LogsDF.cache()
    s3LogsDF
  }
}