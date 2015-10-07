package reinvent.securityanalytics.utilities

import java.util.zip.GZIPInputStream

import com.amazonaws.regions.{Region, RegionUtils}
import com.amazonaws.services.cloudtrail.AWSCloudTrailClient
import com.amazonaws.services.cloudtrail.model.Trail
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.Buffer
import scala.io.Source

/**Functions for loading CloudTrail events from S3*/
object CloudTrailS3Utilities {
  private val logger = LogFactory.getLog(this.getClass)
  private val mapper = new ObjectMapper()

  /**Query CloudTrail to learn where (i.e., which S3 buckets) CloudTrail data is being stored.  If more than one trail
    * logs to the same bucket we will de-dupe so that we don't read from the bucket twice.
    * @return a set of (bucket,prefix) pairs where CloudTrail logs can be found.*/
  def getCloudTrailBucketsAndPrefixes:mutable.Set[(String,String)] = {
    val trailSet:mutable.Set[(String,String)] = new mutable.HashSet[(String,String)] //Sets will remove duplicates

    //For each AWS region, find the CloudTrail trails in that region.
    RegionUtils.getRegions.foreach((region:Region) => {
      try {
        logger.info("Looking for CloudTrail trails in " + region)
        val ct = new AWSCloudTrailClient
        ct.setRegion(region)

        val trails = ct.describeTrails().getTrailList  //Retrieve the list of trails (usually 1 or 0)
        if (trails.size() > 0) {
          logger.info("Trails found: " + trails)
          trails.foreach((trail: Trail) => {
            trailSet.add((trail.getS3BucketName, trail.getS3KeyPrefix))
          })
        }
        else {
          logger.error("No CloudTrail trail configured in " + region + ".  Go turn Cloudtrail on!")
        }
      }
      catch {
        case e: java.lang.Throwable => {
          logger.error("Problem with region: " + region + ". Error: " + e)
          Iterator.empty
        }
      }
    })

    logger.info("Found " + trailSet.size + " S3 buckets and prefixed to explore (after removing duplicates)")
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

    logger.info("Looking in " + bucket + "/" + prefix + " (null means there's no prefix) for CloudTrail logs")
    var objectList = s3.listObjects(bucket, prefix)

    if (objectList.getObjectSummaries.size() > 0) {
      val cloudTrailS3Objects = getCloudTrailS3Keys(objectList.getObjectSummaries) //Get the first batch

      while (objectList.isTruncated) { //If there is more data to be retrieved...
        logger.info("Looking for another batch of S3 objects...")
        objectList = s3.listNextBatchOfObjects(objectList) //... get the next batch ...
        cloudTrailS3Objects ++= getCloudTrailS3Keys(objectList.getObjectSummaries) //... and add it to the original batch
      }
      logger.info("Found " + cloudTrailS3Objects.size + " S3 objects with CloudTrail data")
      cloudTrailS3Objects.map((key:String) => (bucket, key)) //We will need to bucket later, so add it.
    }
    else {
      logger.error("No S3 objects found! bucket=" + bucket + " prefix=" + prefix)
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
      readRawCloudTrailEventsFromS3Object(bucketKeyPair._1, bucketKeyPair._2, s3)
    })
  }

  /**@param bucket the S3 bucket that contains the CloudTrail data
    * @param key the S3 key of the object holding the CloudTrail data
    * @return an iterator over the raw CloudTrail data*/
  def readRawCloudTrailEventsFromS3Object(bucket:String, key:String, s3:AmazonS3Client):Iterator[String] = {
    try {
      Source.fromInputStream(new GZIPInputStream(s3.getObject(bucket, key).getObjectContent)).getLines()
    }
    catch {
      case (e:Exception) => {
        logger.error("Could not read CloudTrail log from s3://" + bucket + "/" + key, e)
        Iterator.empty
      }
    }
  }

  /**Convert a JSON array of CloudTrail events into an iterator of strings where each one is a CloudTrail event
    * @param line the JSON array fo CloudTrail events
    * @return an iterator (iterable, really) over strings where each one is a CloudTrail event*/
  def cloudTrailDataToEvents(line:String):Iterable[String] = {
    val root = mapper.readTree(line) //Find the root node
    val nodeIterator: Iterable[JsonNode] = root.flatMap((n: JsonNode) => n.iterator()) //get an iterator over each Cloudtrail event
    val jsonStrings: Iterable[String] = nodeIterator.map((n: JsonNode) => n.toString) //get the string representation for each event
    jsonStrings
  }

  /** Turn raw CloudTrail data (arrays of events) into one JSON string per event
    * @param rawCloudTrailData CloudTrail strings that have been read from S3 in their original format
    * @return an RDD of strings where each string is an individual CloudTrail event.  This is the format that Spark
    * needs them in. */
  def readCloudtrailRecords(rawCloudTrailData:Iterator[String]):Iterator[String] = {
    val cloudTrailRecords = rawCloudTrailData.flatMap(cloudTrailDataToEvents)
    cloudTrailRecords
  }

  private def isCompressedJson(name:String):Boolean = {
    name.endsWith(".json.gz")
  }
}
