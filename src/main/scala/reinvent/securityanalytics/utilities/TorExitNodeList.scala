package reinvent.securityanalytics.utilities

import java.net.InetAddress

//See TorExitLookup
class TorExitNodeList (val exitNodes:Set[InetAddress], val retrievedTimestamp:Long) extends Serializable
