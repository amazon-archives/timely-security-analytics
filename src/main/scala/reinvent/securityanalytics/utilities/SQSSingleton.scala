package reinvent.securityanalytics.utilities

import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.sqs.AmazonSQSClient

//Make sure we have only SQS client per executor
object SQSSingleton {
  private var _client:Option[AmazonSQSClient] = None
  def client(region:String):AmazonSQSClient = {
    _client match {
      case Some(wrapped) => { wrapped }
      case None => {
        val newClient = new AmazonSQSClient()
        newClient.setRegion(Region.getRegion(Regions.fromName(region)))
        _client = Some(newClient)
        client(region)
      }
    }
  }
}
