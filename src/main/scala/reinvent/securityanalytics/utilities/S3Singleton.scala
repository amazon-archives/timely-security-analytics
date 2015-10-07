package reinvent.securityanalytics.utilities

import com.amazonaws.regions.{Regions,Region}
import com.amazonaws.services.s3.AmazonS3Client

//Make sure we have only one S3 client per executor
object S3Singleton {
  private var _client:Option[AmazonS3Client] = None
  def client(region:String):AmazonS3Client = {
    _client match {
      case Some(wrapped) => { wrapped }
      case None => {
        val newClient = new AmazonS3Client()
        newClient.setRegion(Region.getRegion(Regions.fromName(region)))
        _client = Some(newClient)
        client(region)
      }
    }
  }
}
