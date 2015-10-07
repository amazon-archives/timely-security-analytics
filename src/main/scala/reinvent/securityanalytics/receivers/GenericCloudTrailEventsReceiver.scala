package reinvent.securityanalytics.receivers

import org.apache.commons.logging.LogFactory
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import reinvent.securityanalytics.utilities._

//Common code between receivers for new and existing CloudTrail events
abstract class GenericCloudTrailEventsReceiver(config:Configuration)  extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  protected var run = true
  private var storeCount = 0
  protected val logger = LogFactory.getLog(this.getClass)

  def readAndStoreRawCloudTrailEvents(s3Bucket:String, s3Key:String) = {
    require(s3Bucket.nonEmpty, "Cannot read CloudTrail logs from an empty S3 bucket.")
    require(s3Key.nonEmpty, "Cannot read CloudTrail logs from an empty S3 key.")

    val s3 = S3Singleton.client(config.getString(Configuration.REGION))

    //Pull the raw strings from S3
    val rawEvents = CloudTrailS3Utilities.readRawCloudTrailEventsFromS3Object(s3Bucket, s3Key, s3)
    //Convert these into individual CloudTrail events (JSON strings)
    val cloudTrailEvents = CloudTrailS3Utilities.readCloudtrailRecords(rawEvents)
    cloudTrailEvents.foreach((event:String) => {
      if (run) {
        if (event == null || event.equals("")) {
          logger.warn("I will not store an empty event.")
        }
        else {
          store(event) //Store each event individually
          storeCount += 1
        }
      }
      else {
        logger.error("Not storing due to receiver shutdown.")
      }
    })

    logger.info(storeCount + " CloudTrail events have been read and stored in total..")
  }

  override def onStop():Unit = { run = false}
}
