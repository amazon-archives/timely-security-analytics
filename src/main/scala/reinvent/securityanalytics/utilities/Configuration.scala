package reinvent.securityanalytics.utilities

import java.util.Properties

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.commons.logging.LogFactory

//A serializable wrapper for a Java properties file which contains the application's configuration.
class Configuration(configBucket:String, configKey:String) extends Serializable {
  private val logger = LogFactory.getLog(this.getClass.getSimpleName)
  var _properties:Option[Properties] = None

  def region:Region = {
    Region.getRegion(Regions.fromName(getString(Configuration.REGION)))
  }

  private def properties:Properties = {
    _properties match {
      case None => {
        logger.info("Loading configuration...")
        val s3 = new AmazonS3Client()
        val inputStream = s3.getObject(configBucket, configKey).getObjectContent
        val underlying = new Properties()
        underlying.load(inputStream)
        _properties = Some(underlying)
        underlying
      }
      case Some(underlying) => {
        underlying
      }
    }
  }

  def getString(key:String, isEmptyValueOkay:Boolean = false):String = {
    val value = properties.getProperty(key)
    if (value == null || value.equals("")) {
      if (isEmptyValueOkay) {
        logger.warn("Empty value found for " + key + ".  Returning empty string.")
        ""
      }
      else {
        throw new IllegalArgumentException("Could not find config value for " + key)
      }
    }
    else {
      value
    }
  }

  def getInt(key:String):Int = {
    getString(key).toInt
  }
}

object Configuration {
  val CLOUDTRAIL_BUCKET = "cloudTrailBucket"
  val CLOUDTRAIL_PREFIX = "cloudTrailPrefix"
  val CONFIG_DATA_BUCKET = "configDataBucket"
  val STATE_OUTPUT_BUCKET = "stateOutputBucket"
  val STATE_OUTPUT_PREFIX = "stateOutputPrefix"
  val STATE_OUTPUT_NAME = "stateOutputName"
  val CHECKPOINT_PATH = "checkpointPath"
  val GEO_IP_DB_KEY = "geoIPDatabaseKey"
  val EXIT_NODE_URL = "exitNodeURL"
  val BATCH_INTERVAL_SECONDS = "batchIntervalSeconds"
  val APP_NAME = "appName"
  val REGION = "regionName"
  val ALERT_TOPIC = "alertTopic"
  val ALERT_QUEUE = "alertQueue"
  val CLOUDTRAIL_NEW_LOGS_QUEUE = "cloudTrailQueue"
}