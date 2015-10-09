package reinvent.securityanalytics.receivers

import com.amazonaws.services.sqs.AmazonSQSClient
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec,Matchers}
import org.mockito.Mockito._
import reinvent.securityanalytics.utilities.Configuration
import com.amazonaws.services.sqs.model.Message
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NewCloudTrailEventsReceiverTest extends FlatSpec with Matchers {
  it should "properly parse an SNS message" in {
    val sqsMessageBody = "{\n  \"Type\" : \"Notification\",\n  \"MessageId\" : \"id\",\n  \"TopicArn\" : \"arn:aws:sns:us-west-2::cloudTrailLogArrivals\",\n  \"Message\" : \"{\\\"s3Bucket\\\":\\\"cloudtrail17\\\",\\\"s3ObjectKey\\\":[\\\"objectPath\\\"]}\",\n  \"Timestamp\" : \"2015-10-03T01:36:22.964Z\",\n  \"SignatureVersion\" : \"1\",\n  \"Signature\" : \"AAAAA\",\n  \"SigningCertURL\" : \"https://sns.us-west-2.amazonaws.com/SimpleNotificationService-bb750dd426d95ee9390147a5624348ee.pem\",\n  \"UnsubscribeURL\" : \"url\"\n}"
    val receipt = "RECEIPT"
    val queueURL = "URL"
    val sqs = mock(classOf[AmazonSQSClient])
    val config = mock(classOf[Configuration])
    when(config.getString(Configuration.CLOUDTRAIL_NEW_LOGS_QUEUE)).thenReturn(queueURL)
    val sqsMessage = mock(classOf[Message])
    when(sqsMessage.getBody).thenReturn(sqsMessageBody)
    when(sqsMessage.getReceiptHandle).thenReturn(receipt)

    var stored = false
    def readAndStoreFunction(bucket:String, key:String):Unit = {
      stored = true
    }

    val receiver = new NewCloudTrailEventsReceiver(config)
    receiver.processSNSMessageInSQSMessage(sqsMessage, sqs, readAndStoreFunction)

    assert(stored)
    verify(sqs).deleteMessage(queueURL, receipt)
  }
}