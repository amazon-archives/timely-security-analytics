package reinvent.securityanalytics.profilers

import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkContext, Logging}
import reinvent.securityanalytics.state.ActivityProfile
import reinvent.securityanalytics.utilities.{Configuration, SNSSingleton}

/**ActivityProfiler is the most generic "profile" builder.  It contains most of the comparison and alerting loci.
  * @param key a string that uniquely identifies this profiler*/
class ActivityProfiler[T](val key:String, config:Configuration) extends Serializable {
  private val logger = LogFactory.getLog(this.getClass)
  private val EOL = "\r\n"
  protected var alerts = 0

  protected def updateProfile(newActivity:Set[T], previousProfile:ActivityProfile[T]):ActivityProfile[T] = {
    logger.info("Updating profile for " + key)

    if (newActivity != null && newActivity.nonEmpty) {
      val oldSize = previousProfile.activity.size

      //Add new activity to the existing profile //TODO Handle expiration in cases where it makes sense
      val newProfile:ActivityProfile[T] = previousProfile.addNewActivity(newActivity)

      val newSize = newProfile.activity.size
      if (newSize <= oldSize) {
        logger.error("Problem: profile did not grow. " + oldSize + " vs. " + newSize)
      }
      logger.info("Saved " + newProfile + " as profile for " + key)

      newProfile
    }
    else {
      logger.error("Empty profile found for " + key )
      new ActivityProfile(Set.empty)
    }
  }

  //Override this if you need to transform the input in some way
  def compareNewActivity(newActivity:Set[T], profile:ActivityProfile[T]):ActivityProfile[T] = {
    compareNewTransformedActivity(newActivity, profile)
  }

  /*compareNewTransformedActivity will compare new (transformed) activity against the current profile*/
  protected def compareNewTransformedActivity(newActivity:Set[T], profile:ActivityProfile[T]):ActivityProfile[T] = {
    if (profile.activity.isEmpty) { //If no profile exists, we'll create one from this batch of data
      logger.info("Empty profile found for " + key + ". We will create a new profile using " + newActivity)
      val newProfile = new ActivityProfile(newActivity)
      logger.info("New profile" + newProfile)
      newProfile
    }
    else {
      if (newActivity.subsetOf(profile.activity)) {
        logger.info("The following activity is In compliance with our profile for " + key + ": "
          + newActivity.mkString(","))
        profile //Return the existing profile as the new state
      }
      else { //We have a deviation from the profile.  Send an alert and update the profile.
        alert(newActivity, profile)
        updateProfile(newActivity, profile)
      }
    }
  }

  //Logic for generating an SNS alert
  private def alert(newActivity:Set[T], profile:ActivityProfile[T]): Unit = {
    val intersection = profile.activity.intersect(newActivity)
    val sns = SNSSingleton.client(config.getString(Configuration.REGION))
    val subject = "CloudTrail Profile Mismatch for " + key
    val body = new StringBuilder()
    body.append("New activity which doesn't fit current profile: " +
      newActivity.diff(profile.activity).mkString(",") + EOL)
    body.append(EOL + "Current activity (including compliant activity): " + newActivity.mkString(",") + EOL)
    body.append("Current profile (which was not matched): " + profile.activity.mkString(",") + EOL)
    body.append("The following is the intersection between the existing profile and new activity: " +
      intersection.mkString(",") + EOL)
    body.append(EOL + "The profile will be updated (to avoid duplicate alarms)")

    logger.info("***** Sending alert: Subject="+subject+" Body="+body.toString())
    sns.publish(config.getString(Configuration.ALERT_TOPIC), body.toString(), subject)
    alerts += 1
  }

  override def toString():String = {
    key + " generated " + alerts + " alerts."
  }

  //Child classes should override this
  def initialize(sparkContext:SparkContext):Unit = {}
}