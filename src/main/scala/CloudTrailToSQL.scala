import java.util.zip.GZIPInputStream
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.cloudtrail.AWSCloudTrailClient
import com.amazonaws.services.cloudtrail.model.Trail
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, Logging}
import com.amazonaws.regions.Region
import scala.collection.mutable
import scala.collection.mutable.Buffer
import scala.io.Source
import scala.collection.JavaConversions._

/*Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
  the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
  CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
  and limitations under the License.*/

/** Do you want to run SQL queries over your CloudTrail logs?  Well, you're in the right place.  This code is written
  * so you can cut-and-paste it into a spark-shell and then start running SQL queries (hence why there is no package
  * in this code and I've used as few libraries as possible).  All the libraries it requires are already included in
  * the build of Spark 1.4.1 that's available through Amazon's Elastic MapReduce (EMR).  For more information, see
  *  https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/
  *
  * Quick Start: To use this code, do the following:
  * 1. Provision an EMR cluster with Spark 1.4.1 and an IAM role that has CloudTrail and S3 describe and read permissions.
  * 2. SSH to that cluster and run that spark-shell, e.g.:
      spark-shell --master yarn-client --num-executors 40 --conf spark.executor.cores=2
  * 3. Cut and paste all of this code into your Spark Shell (once the scala> prompt is available)
  * 4. Run the following commands:
      var cloudtrail = CloudTrailToSQL.createTable(sc, sqlContext) //creates and registers the Spark SQL table
      CloudTrailToSQL.runSampleQuery(sqlContext) //runs a sample query

    Note that these commands will take some time to run as they load your CloudTrail data from S3 and store it in-memory
     on the Spark cluster.  Run the sample query again and you'll see the speed up that the in-memory caching provides.
  * 5. Run any SQL query you want over the data, e.g.
    sqlContext.sql("select distinct eventSource, eventName, userIdentity.principalId from cloudtrail where userIdentity.principalId = userIdentity.accountId").show(99999) //Find services and APIs called with root credentials
  */
object CloudTrailToSQL extends Logging {
  private val cloudTrailTableName = "cloudtrail"
  
  /**Query CloudTrail to learn where (i.e., which S3 buckets) CloudTrail data is being stored.  If more than one trail 
    * logs to the same bucket we will de-dupe so that we don't read from the bucket twice.
   * @return a set of (bucket,prefix) pairs where CloudTrail logs can be found.*/
  def getCloudTrailBucketsAndPrefixes:mutable.Set[(String,String)] = {
    val trailSet:mutable.Set[(String,String)] = new mutable.HashSet[(String,String)] //Sets will remove duplicates

    //For each AWS region, find the CloudTrail trails in that region.
    RegionUtils.getRegions.foreach((region:Region) => {
      try {
        logInfo("Looking for CloudTrail trails in " + region)
        val ct = new AWSCloudTrailClient
        ct.setRegion(region)

        val trails = ct.describeTrails().getTrailList  //Retrieve the list of trails (usually 1 or 0)
        if (trails.size() > 0) {
          logInfo("Trails found: " + trails)
          trails.foreach((trail: Trail) => {
            trailSet.add((trail.getS3BucketName, trail.getS3KeyPrefix))
          })
        }
        else {
          logError("No CloudTrail trail configured in " + region + ".  Go turn Cloudtrail on!")
        }
      }
      catch {
        case e: java.lang.Throwable => {
          logError("Problem with region: " + region + ". Error: " + e)
          Iterator.empty
        }
      }
    })

    logInfo("Found " + trailSet.size + " S3 buckets and prefixed to explore (after removing duplicates)")
    trailSet
  }

  /** See getCloudTrailBucketsAndPrefixes except that we will return a list of all S3 objects that have CloudTrail data
    * @return a set of (bucket, S3 key) pairs that contain CloudTrail data.*/
  def getCloudTrailS3Objects:mutable.Set[(String,String)] = {
    val trailSet = getCloudTrailBucketsAndPrefixes
    getCloudTrailS3Objects(trailSet)
  }

  /** See getCloudTrailBucketsAndPrefixes except that we will return a list of all S3 objects that have CloudTrail data
    * @param trailSet a set of (bucket,prefix) pairs to be explored for CloudTrail data
    * @return a set of (bucket, S3 key) pairs that contain CloudTrail data.*/
  def getCloudTrailS3Objects(trailSet:mutable.Set[(String,String)]):mutable.Set[(String,String)] = {
    trailSet.flatMap((trailS3Info:(String,String)) => {
      findCloudTrailDataInBucket(trailS3Info._1, trailS3Info._2)
    })
  }

  /**For a given bucket, go find the keys under the given prefix.  S3 returns results in batches so this will page
    * through the batches and return the full set of keys
    * @param bucket the S3 bucket in which to look for CloudTraildata
    * @param prefix the S3 prefix in that bucket to look for CloudTrail data
    * @return a buffer of (bucket, S3 key) pairs that contain CloudTrail data.*/
  def findCloudTrailDataInBucket(bucket:String, prefix:String):mutable.Buffer[(String,String)] = {
    /*We create a new S3 client each time this method is called.  This is necessary because the AmazonS3Client is not
    * serializable to Spark executors.  An improvement is to use a proxy pattern that returns a singleton instead of
    * creating a new client.  However, to keep it simple, we're just going to stick with one client per bucket. */
    val s3 = new AmazonS3Client  

    logInfo("Looking in " + bucket + "/" + prefix + " (null means there's no prefix) for CloudTrail logs")
    var objectList = s3.listObjects(bucket, prefix)

    if (objectList.getObjectSummaries.size() > 0) {
      val cloudTrailS3Objects = getCloudTrailS3Keys(objectList.getObjectSummaries) //Get the first batch

      while (objectList.isTruncated) { //If there is more data to be retrieved...
        logInfo("Looking for another batch of S3 objects...")
        objectList = s3.listNextBatchOfObjects(objectList) //... get the next batch ...
        cloudTrailS3Objects ++= getCloudTrailS3Keys(objectList.getObjectSummaries) //... and add it to the original batch
      }
      logInfo("Found " + cloudTrailS3Objects.size + " S3 objects with CloudTrail data")
      cloudTrailS3Objects.map((key:String) => (bucket, key)) //We will need to bucket later, so add it.
    }
    else {
      logError("No S3 objects found! bucket=" + bucket + " prefix=" + prefix)
      Buffer.empty
    }
  }

  /** Take an S3 object list, remove irrelevant objects, and extract the key.
    * @param objectList an object list returned by S3
    * @return a buffer (list) of S3 keys */
  def getCloudTrailS3Keys(objectList:java.util.List[S3ObjectSummary]):Buffer[String] = {
    objectList.filter((summary: S3ObjectSummary) => {
      isCompressedJson(summary.getKey)
    }).map((summary:S3ObjectSummary) => {
      summary.getKey
    })
  }

  /** Starting from a SparkContext, retrieve the raw CloudTrail data
    * @param sparkContext the sparkContext
    * @return an RDD of raw CloudTrail strings*/
  def loadFromS3(sparkContext:SparkContext):RDD[String] = {
    val list:mutable.Set[(String,String)] = getCloudTrailS3Objects
    val rdd:RDD[(String,String)] = sparkContext.parallelize(list.toSeq)
    loadFromS3(rdd)
  }

  /** Starting from an RDD of (bucket,key) pairs, retrieve the raw CloudTrail data
    * @param bucketKeyPairs the S3 (bucket,key) pairs that contain CloudTrail data
    * @return an RDD of raw CloudTrail strings*/
  def loadFromS3(bucketKeyPairs:RDD[(String,String)]):RDD[String] = {
    bucketKeyPairs.flatMap(bucketKeyPair => {
      val s3 = new AmazonS3Client()
      Source.fromInputStream(new GZIPInputStream(s3.getObject(bucketKeyPair._1, bucketKeyPair._2).getObjectContent)).getLines()
    })
  }

  /** Turn raw CloudTrail data (arrays of events) into one JSON string per event
    * @param rawCloudTrailData CloudTrail strings that have been read from S3 in their original format
    * @return an RDD of strings where each string is an individual CloudTrail event.  This is the format that Spark
    * needs them in. */
  def readCloudtrailRecordsFromRDD(rawCloudTrailData:RDD[String]):RDD[String] = {
    val cloudTrailRecords = rawCloudTrailData.flatMap((line:String) => {
      val mapper = new ObjectMapper() //A singleton pattern might be beneficial here as well.
      val root = mapper.readTree(line) //Find the root node

      //Get an iterator over each Cloudtrail event
      val nodeIterator: Iterable[JsonNode] = root.flatMap((n: JsonNode) => n.iterator())

      //Get the string representation for each event
      val jsonStrings: Iterable[String] = nodeIterator.map((n: JsonNode) => n.toString)
      jsonStrings
    })
    cloudTrailRecords.persist() //Store these events in memory so the first query doesn't have to re-fetch from S3
    cloudTrailRecords
  }

  /**Main entry point: convert all CloudTrail logs into a Spark DataFrame, register it as a table
    * @param sc the Spark Context
    * @param sqlContext the SQL Context
    * @return a DataFrame that represents all your CloudTrail logs*/
  def createTable(sc:SparkContext, sqlContext:SQLContext):DataFrame = {
    val rawCloudTrailData:RDD[String] = loadFromS3(sc)
    val individualCloudTrailEvents:RDD[String] = readCloudtrailRecordsFromRDD(rawCloudTrailData)
    val cloudtrailRecordsDataFrame:DataFrame = sqlContext.read.json(individualCloudTrailEvents)
    cloudtrailRecordsDataFrame.cache() //After your first query, all data will be cached in memory

    //Enable querying as the given table name via the SQL context
    cloudtrailRecordsDataFrame.registerTempTable(cloudTrailTableName)
    cloudtrailRecordsDataFrame
  }

  def createHiveTable(sc:SparkContext, hiveContext:SQLContext):DataFrame = {
    require(hiveContext.isInstanceOf[HiveContext], "You must pass a SQL context that is a HiveContext.  Use the sqlContext val created for you.")
    val rawCloudTrailData = loadFromS3(sc)
    val individualCloudTrailEvents = readCloudtrailRecordsFromRDD(rawCloudTrailData)
    val hiveCloudTrailDataFrame = hiveContext.read.json(individualCloudTrailEvents)
    hiveCloudTrailDataFrame.cache()
    hiveCloudTrailDataFrame.write.saveAsTable(cloudTrailTableName+"hive")
    hiveCloudTrailDataFrame
  }

  def runSampleQuery(sqlContext:SQLContext) = {
    sqlContext.sql("select distinct userIdentity.principalId, sourceIPAddress, userIdentity.accessKeyId from " + cloudTrailTableName + " order by accessKeyId").show(10000)
  }

  private def isCompressedJson(name:String):Boolean = {
    name.endsWith(".json.gz")
  }
}
