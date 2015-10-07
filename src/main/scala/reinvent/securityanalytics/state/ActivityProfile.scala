package reinvent.securityanalytics.state

/**ActivityProfile contains a set which is the profile of the previous activity and a count of how many
  * alerts it has sent.*/
class ActivityProfile[T] (previousActivity:Set[T], val alertCount:Int = 0) extends Serializable {

  //Union the old and new activity and increment the alert count
  def addNewActivity(newActivity:Set[T]):ActivityProfile[T] = {
    new ActivityProfile(previousActivity ++ newActivity, alertCount + 1)
  }

  def activity:Set[T] = previousActivity

  override def toString:String = {
    val stringBuffer = new StringBuffer()
    stringBuffer.append("Profile contents are " + previousActivity.mkString(",") + " (Size=" +
      previousActivity.size + ")\n")
    stringBuffer.append("Count of alerts sent: " + alertCount + "\n")
    stringBuffer.toString
  }
}
