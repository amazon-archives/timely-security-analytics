package reinvent.securityanalytics.receivers

import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.Message
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.TextNode
import org.apache.commons.logging.LogFactory
import reinvent.securityanalytics.utilities._
import scala.collection.JavaConversions._

/**NewCloudTrailEventsReceiver will receive CloudTrail event notifications for new log arrivals, go find those
  * logs, parse out the events, and send them along for processing. */
class NewCloudTrailEventsReceiver(config:Configuration) extends GenericCloudTrailEventsReceiver(config) {
  private val noMessageSleepTimeMillis = 15000 //wait 15 seconds between empty message batches
  private val SNS_MESSAGE_FIELD_IN_SQS = "Message"
  private val S3_BUCKET_FIELD_IN_SNS = "s3Bucket"
  private val S3_OBJECT_FIELD_IN_SNS = "s3ObjectKey"
  override protected val logger = LogFactory.getLog(this.getClass)

  override def onStart():Unit = {
    val sqs = SQSSingleton.client(config.getString(Configuration.REGION))

    while (run) {
      val sqsMessages = sqs.receiveMessage(config.getString(Configuration.CLOUDTRAIL_NEW_LOGS_QUEUE)).getMessages
      if (sqsMessages.size() < 1) {
        logger.info("No messages are available so we'll take a short nap so we don't hammer SQS.")
        Thread.sleep(noMessageSleepTimeMillis)
      }
      else {
        //We get a batch of SQS messages back
        sqsMessages.foreach(processSNSMessageInSQSMessage(_, sqs))
      }
    }
  }

  def processSNSMessageInSQSMessage(sqsMessage:Message, sqs:AmazonSQSClient):Unit = {
    processSNSMessageInSQSMessage(sqsMessage, sqs, readAndStoreRawCloudTrailEvents)
  }

  /* SNS notifications sent to SQS look like:
  {
    ...
    "Message" : "{\"s3Bucket\":\"cloudtrail17\",\"s3ObjectKey\":[\"pathToCloudTrailObject.json.gz\"]}",
    ...
  }
  See http://docs.aws.amazon.com/awscloudtrail/latest/userguide/configure-cloudtrail-to-send-notifications.html
* */
  def processSNSMessageInSQSMessage(sqsMessage:Message, sqs:AmazonSQSClient, readAndStore:(String,String)=>Unit):Unit = {
    val sqsMessageBody = sqsMessage.getBody
    //The body is a JSON object, so find the root of the object
    val sqsMessageRoot = ObjectMapperSingleton.mapper.readTree(sqsMessageBody)

    //Find the SNS message in the SQS message
    val snsMessages = sqsMessageRoot.findValuesAsText(SNS_MESSAGE_FIELD_IN_SQS)

    if (snsMessages.size < 1) {
      logger.info("Could not find SNS message in SQS message: " + sqsMessageBody)
    }
    else {
      //There should only be one SNS message, but in case there are more we are doing foreach
      snsMessages.foreach(processNewCloudTrailLogsSNSNotification(_, sqsMessage, sqs, readAndStore))
    }
  }

  //Find and process the notification in the SNS message
  def processNewCloudTrailLogsSNSNotification(snsMessage:String, sqsMessage:Message, sqs:AmazonSQSClient, readAndStore:(String, String)=>Unit) {
    val sqsMessageBody = sqsMessage.getBody
    //Find the root of the SNS message, which is also a JSON object
    val snsMessageRoot = ObjectMapperSingleton.mapper.readTree(snsMessage)
    val s3BucketList = snsMessageRoot.findValuesAsText(S3_BUCKET_FIELD_IN_SNS) //Find the bucket name
    if (s3BucketList.size() != 1) {
      logger.error("Was expecting to find 1 S3 bucket and found " + s3BucketList.size() + " instead in " + sqsMessageBody)
    } else {
      val s3Bucket = s3BucketList.head //Get the one bucket name

      val s3ObjectListNode = snsMessageRoot.get(S3_OBJECT_FIELD_IN_SNS)
      val s3ObjectList = s3ObjectListNode.elements().toSet
      if (s3ObjectList.size < 1) {
        logger.error("Found empty S3 object list in " + sqsMessageBody)
      }
      else {
        s3ObjectList.foreach((node:JsonNode) => {
          if (node.isInstanceOf[TextNode]) {
            val s3Key = node.asInstanceOf[TextNode].textValue()
            //Get and store individual CloudTrail events
            readAndStore(s3Bucket, s3Key)

            //Delete the message since we've successfully processed it.
            sqs.deleteMessage(config.getString(Configuration.CLOUDTRAIL_NEW_LOGS_QUEUE), sqsMessage.getReceiptHandle)
          }
          else {
            logger.error("Could not read S3 object key in " + node)
          }
        })
      }
    }
  }
}
