package reinvent.securityanalytics.receivers

import com.amazonaws.services.s3.AmazonS3Client
import org.apache.commons.logging.LogFactory
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import reinvent.securityanalytics.utilities.{Configuration, CloudTrailS3Utilities}

/**One of the best ways to test this code is to replay your existing CloudTrail records as if they arrived fresh.
  *  This receiver does that by looking in the configured bucket, with the configured prefix, for CloudTrail logs.
  *  As it finds them, it converts them to the raw events and store()s them.  Control the incoming rate with the
  *  spark.streaming.receiver.maxRate Spark configuration value. */
class ReplayExistingCloudTrailEventsReceiver(config:Configuration) extends GenericCloudTrailEventsReceiver(config) {
  override protected val logger = LogFactory.getLog(this.getClass)

  override def onStart():Unit = {
    val bucket = config.getString(Configuration.CLOUDTRAIL_BUCKET)
    val prefix = config.getString(Configuration.CLOUDTRAIL_PREFIX, true)
    val s3 = new AmazonS3Client()
    val cloudTrailS3Objects = CloudTrailS3Utilities.findCloudTrailDataInBucket(bucket, prefix)

    //For each S3 object containing CloudTrail data...
    cloudTrailS3Objects.foreach(bucketKeyPair => {
      readAndStoreRawCloudTrailEvents(bucketKeyPair._1, bucketKeyPair._2)
    })

    logger.error("Finished reading existing CloudTrail events.")
  }
}

