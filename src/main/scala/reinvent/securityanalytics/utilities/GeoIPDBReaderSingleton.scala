package reinvent.securityanalytics.utilities

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3Client
import com.maxmind.geoip2.DatabaseReader
import org.apache.spark.Logging

import scala.util.{Failure, Success, Try}

/**Make sure we load the DB once, and only once per executor.*/
object GeoIPDBReaderSingleton extends Logging {
  private var _dbreader:Option[DatabaseReader] = None

  /** @return the DatabaseReader singleton that can be used to access the GeoIP database*/
  def dbreader(config:Configuration):DatabaseReader = {
    _dbreader match {
      case None => {
        loadDBFromS3(config) match {
          case Success(db) => {
            logInfo("Creating new GeoIP DB reader")
            _dbreader = Some(new DatabaseReader.Builder(db).build())
          }
          case Failure(e) => {
            throw new GeoIPException("Geo IP DB could not be loaded.  Make sure you have it specified in your config.", e)
          }
        }
        dbreader(config)
      }
      case Some(reader) => reader
    }
  }

  /**Attempt to load the database from a location in S3
    * @return a Success(inputstream) in the event of success or a Failure wrapping the exception*/
  def loadDBFromS3(config:Configuration):Try[InputStream] = {
    val s3 = new AmazonS3Client()
    Success(s3.getObject(config.getString(Configuration.CONFIG_DATA_BUCKET), config.getString(Configuration.GEO_IP_DB_KEY)).getObjectContent)
  }
}