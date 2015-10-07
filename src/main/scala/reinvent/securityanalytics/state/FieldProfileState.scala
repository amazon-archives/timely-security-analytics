package reinvent.securityanalytics.state

/* We want to use Spark's DStream.updateStateByKey functionality for scalably comparing historical data against
* new data.  Doing so requires us to maintain state for each of the CloudTrail fields we want to keep track of.
* This state is the values of the CloudTrail field that we've seen in the past.  In some cases (e.g., SourceIPAddress),
* we may transform the CloudTrail values (e.g., look up the GeoIP country of origin) and compare those transformed
* values against any previous values.  Therefore, we have a one-to-many mapping of fields to the profiles we want to
* keep for them.  FieldProfileState contains this mapping.  Specifically, it maps the profiler name to the set of
* previously seen values.  Therefore, updateStateByKey will take the new activity, look up the profilers for the field,
* and pass the new data to each profiler.  Each profiler will return the new profile (or state) to be saved for that
* profiler and we will store that state in the FieldProfileState map.*/

class FieldProfileState(stateMappings:Map[String, ActivityProfile[String]], updateCount:Int = 0) extends Serializable {
  def updateProfilerStateMappings(newMappings:Map[String, ActivityProfile[String]]):FieldProfileState = {
    new FieldProfileState(newMappings, updateCount + 1)
  }

  def mappings = stateMappings

  override def toString:String = {
    val stringBuffer = new StringBuffer()
    stringBuffer.append("This field's state has been updated " + updateCount + " times.\n")
    stringBuffer.append("State mappings size is " + stateMappings.size + ".  State mappings are:\n")
    stateMappings.foreach(pair => {
      val profilerName = pair._1
      val profile:ActivityProfile[String] = pair._2
      stringBuffer.append("Profile for " + profilerName + " is " + profile.toString + "\n")
    })
    stringBuffer.toString
  }
}

