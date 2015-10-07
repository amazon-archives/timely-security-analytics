package reinvent.securityanalytics.utilities

import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.sns.AmazonSNSClient

//Make sure we have only one SNS client per executor
object SNSSingleton {
  private var _client:Option[AmazonSNSClient] = None
  def client(region:String):AmazonSNSClient = {
    _client match {
      case Some(wrapped) => { wrapped }
      case None => {
        val newClient = new AmazonSNSClient()
        newClient.setRegion(Region.getRegion(Regions.fromName(region)))
        _client = Some(newClient)
        client(region)
      }
    }
  }
}
