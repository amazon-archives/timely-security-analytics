package reinvent.securityanalytics.utilities

class GeoIPException(message:String, underlying:Throwable) extends Exception(message, underlying) with Serializable