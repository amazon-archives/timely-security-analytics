package reinvent.securityanalytics

import java.net.{InetAddress, UnknownHostException}

import com.maxmind.geoip2.model.CityResponse
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SQLContext
import reinvent.securityanalytics.utilities.{Configuration, GeoIPDBReaderSingleton}

class GeoIPLookup(config:Configuration) extends Serializable {
  private val logger = LogFactory.getLog(this.getClass)

  /**@param ipAddress the IP address for which to look up the city
    * @return the city name for the IP address*/
  def cityLookup(ipAddress:String):String = {
    try {
      ipGeoLocationLookup(ipAddress).getCity.getName
    }
    catch {
      case (u:UnknownHostException) => {
        logger.error("Invalid IP address: " + ipAddress, u)
        "Unknown"
      }
      case (e:Exception) => {
        logger.error("Unexpected exception while parsing IP address", e)
        "Unknown"
      }
    }
  }

  /**@param ipAddress the IP address for which to look up the country
    * @return the country for the IP address*/
  def countryLookup(ipAddress:String):String = {
    try {
      ipGeoLocationLookup(ipAddress).getCountry.getName
    }
    catch {
      case (u:Throwable) => {
        logger.error("Invalid IP address: " + ipAddress, u)
        "Unknown"
      }
      case (e:Exception) => {
        logger.error("Unexpected exception while parsing IP address", e)
        "Unknown"
      }
    }
  }

  /**@param ipAddress the IP address string for which to look up the city
    * @return the CityResponse object for that string*/
  def ipGeoLocationLookup(ipAddress:String):CityResponse = {
    ipGeoLocationLookup(InetAddress.getByName(ipAddress))
  }

  /**@param inetAddress the IP address object for which to look up the city
    * @return the CityResponse object for that string*/
  def ipGeoLocationLookup(inetAddress:InetAddress):CityResponse = {
    GeoIPDBReaderSingleton.dbreader(config).city(inetAddress)
  }

  /**Registers user-defined functions in the SQL context for querying GeoIP data*/
  def registerUDFs(sqlContext:SQLContext):Unit = {
    sqlContext.udf.register("city", cityLookup(_:String))
    sqlContext.udf.register("country", countryLookup(_:String))
  }

  //Can only be run after registerUDFs is called, otherwise UDFs won't be defined.
  def runSampleGeoIPQuery(sqlContext:SQLContext) = {
    sqlContext.sql("select distinct sourceIpAddress, city(sourceIpAddress) as city, country(sourceIpAddress) as country from cloudtrail").show(10000)
  }
}