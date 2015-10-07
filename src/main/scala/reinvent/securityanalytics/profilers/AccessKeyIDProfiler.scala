package reinvent.securityanalytics.profilers

import reinvent.securityanalytics.state.{ActivityProfile, AWS_ACCESS_KEY_ID}
import reinvent.securityanalytics.utilities.Configuration

/** AccessKeyIDProfiler will build a profile of the activity of long-term AWS access keys IDs
  * Access Key IDs beginning in "ASIA" will be ignored as they are temporary and will result in
  * significant false positives. */
class AccessKeyIDProfiler(config:Configuration) extends ActivityProfiler[String] (AWS_ACCESS_KEY_ID.name, config) {
  override def compareNewActivity(newActivity:Set[String], profile:ActivityProfile[String]):ActivityProfile[String] = {
    val transformedNewActivity = newActivity.filter((accessKeyId:String) => !accessKeyId.startsWith("ASIA"))
    if (transformedNewActivity.nonEmpty) {
      compareNewTransformedActivity(transformedNewActivity, profile)
    }
    else { //Return the current profile
      profile
    }
  }
}
