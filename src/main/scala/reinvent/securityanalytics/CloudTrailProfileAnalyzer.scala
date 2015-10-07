package reinvent.securityanalytics

import org.apache.commons.logging.LogFactory
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import reinvent.securityanalytics.profilers._
import reinvent.securityanalytics.receivers.{NewCloudTrailEventsReceiver, ReplayExistingCloudTrailEventsReceiver}
import reinvent.securityanalytics.state._
import reinvent.securityanalytics.utilities._
import scala.collection.JavaConversions._
import scala.collection.mutable

/*Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
  the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
  CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
  and limitations under the License.*/

/**CloudTrailProfileAnalyzer uses Spark Streaming to analyze CloudTrail data as soon as it is available in S3.  It uses
  * this data to build a "profile" of the activity seen to date and then checks new activity against this profile.  When
  * the new activity doesn't match the historical activity, it sends an alert to the SNS topic specified in the
  * configuration.
  *
  * The stock profiling logic is encapsulated in "profilers," specifically the following:
  *  AccessKeyIDProfiler will alert you when a new long-term (i.e., not temporary) access key is used with your account.
  *    This may be interesting to you if you want to learn about keys which were dormant (i.e., unused) that are now in
  *    use.
  *  A GenericCloudTrailProfiler will alert you when a new principal, as identified by the ARN, interacts with your
  *    AWS resources.
  *  A GenericCloudTrailProfiler will alert you when a new principal, as identified by the Principal ID, interacts with
  *    your AWS resources.  "Why do you have two different principal ID alerts?"  The alerts provide different resolution.
  *    The ARN alert will alert you when a new role name is used with a role principal ID (i.e., ARO...) whereas the
  *    principal ID alert will not.  If you'd like more details, use the principal ARN alert.  If you'd like fewer alerts,
  *    potentially at the expense of missing important details, use the principal ID alert.
  *  A GenericCloudTrailProfiler will alert you when AWS activity involving your AWS resources comes from a previously-
  *    unseen Source IP Address.
  *  A GeoIPCityProfiler will alert you when AWS activity involving your AWS resources comes from a new city, as
  *    determined by a GeoIP database.
  *  A GeoIPCountryProfiler will alert you when AWS activity involving your AWS resources comes from a new country, as
  *    determined by a GeoIP database.
  *  A TorProfiler which will alert you when AWS activity involving your AWS resources originates from the Tor anonymizing
  *    network.  While this may be fine for some administrators, this may be unacceptable to others who would see the
  *    use of an anonymizing proxy as a sign of hiding one's tracks.
  *
  * You can either build simple profiles using profile.GenericCloudTrailProfiler or you can build custom profiles by
  * extending either profile.ActivityProfiler or profile.GenericCloudTrailProfiler.  Other profiler ideas include:
  *  Alerts when a root account is used to access your account's resources. */
object CloudTrailProfileAnalyzer {
  private val logger = LogFactory.getLog(this.getClass)

  def main(args:Array[String]) = {
    require(args.length == 2, "You must supply the S3 bucket and key to a config file")
    val config = new Configuration(args(0), args(1))

    //Each field we're interested in profiling maps to one or more profilers to be applied to it
    val fieldToProfilerMap:Map[Field, Set[ActivityProfiler[String]]] = Map(
      AWS_ACCESS_KEY_ID -> Set(new AccessKeyIDProfiler(config)),
      PRINCIPAL_ARN -> Set(new GenericCloudTrailProfiler[String](PRINCIPAL_ARN, config)),
      SOURCE_IP_ADDRESS -> Set(
        new GeoIPCityProfiler(config),
        new GeoIPCountryProfiler(config),
        new TorProfiler(config))
    )

    def createStreamingContextFunction = {
      createStreamingContext(config, fieldToProfilerMap)
    }

    val streamingContext = StreamingContext.getOrCreate(config.getString(Configuration.CHECKPOINT_PATH),
      createStreamingContextFunction _, new org.apache.hadoop.conf.Configuration(),
      true //Start application even if checkpoint restore fails
    )

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def createStreamingContext(config:Configuration,
                             fieldToProfilerMap:Map[Field, Set[ActivityProfiler[String]]]):StreamingContext = {
    val sparkConf = new SparkConf().setAppName(config.getString(Configuration.APP_NAME))

    val duration = Seconds(config.getInt(Configuration.BATCH_INTERVAL_SECONDS))

    val streamingContext = new StreamingContext(sparkConf, duration) //Set our batch interval

    //Make sure the streaming application can recover after failure
    val checkpointPath = config.getString(Configuration.CHECKPOINT_PATH) + "/" + System.currentTimeMillis()
    //TODO right now we are creating a new checkpoint per run.  This is to work around an error in Spark.  To restore
    // from this checkpoint, you will need to modify this code.
    streamingContext.checkpoint(checkpointPath)

    val sparkContext = streamingContext.sparkContext

    //Initialize any profilers that require initialization
    initializeProfilers(fieldToProfilerMap, sparkContext)

    //Create DStream of CloudTrail events from receivers
    val cloudTrailEventsStream:DStream[String] = createDStream(streamingContext, config)

    //Create empty initial state
    val initialState = createEmptyInitialState(sparkContext, fieldToProfilerMap)

    //Profile incoming CloudTrail data
    val stateDStream = profileCloudTrailEvents(cloudTrailEventsStream, sparkContext, initialState, fieldToProfilerMap)
    stateDStream.checkpoint(duration)

    //Output the state after the current batch
    outputStatusOfProfilersAndProfiles(sparkContext, config, stateDStream)

    streamingContext
  }

  //Perform any profiler initialization that may be necessary
  def initializeProfilers(fieldToProfilerMap:Map[Field, Set[ActivityProfiler[String]]], sparkContext:SparkContext) = {
    fieldToProfilerMap.foreach(pair => {
      val profilerSet = pair._2
      profilerSet.foreach((profiler:ActivityProfiler[String]) => {
        profiler.initialize(sparkContext)
      })
    })
  }

  def createDStream(streamingContext:StreamingContext, config:Configuration):DStream[String] = {
    //Configure our receiver
    val existingEventsReceiver = new ReplayExistingCloudTrailEventsReceiver(config)
    val newEventsReceiver = new NewCloudTrailEventsReceiver(config)

    val existingEventsStream = streamingContext.receiverStream(existingEventsReceiver)
    val newEventsStream = streamingContext.receiverStream(newEventsReceiver)

    //Union the two event streams so we have one event stream.
    existingEventsStream.union(newEventsStream)
  }

  /*Output the number of alerts for each profiler (and the fields they are profiling) that have been issued.  This
 * serves two purposes.  First, the operator can evaluate the effectiveness the frequency of alerts.  Next, the
 * foreachRDD causes the CloudTrail data to be read and processed.  Without this, nothing would happen due to
 * Spark's lazy loading. */
  def outputStatusOfProfilersAndProfiles(sparkContext:SparkContext, config:Configuration,
                                      stateDStream:DStream[(Field, FieldProfileState)]):Unit = {
    val profileAlertStatusAccumulator = sparkContext.accumulableCollection(new mutable.HashSet[String])

    stateDStream.foreachRDD(fieldStateRDD => {
      fieldStateRDD.foreach((fieldStatePair) => {
        val field = fieldStatePair._1
        val fieldState = fieldStatePair._2
        fieldState.mappings.foreach(profilerProfilePair => {
          val profilerName = profilerProfilePair._1
          val currentProfile = profilerProfilePair._2
          val alertCount = currentProfile.alertCount

          val statusMessage = "The " + profilerName + " profiler for the field " + field.name + " has issued " + alertCount + " alerts."
          profileAlertStatusAccumulator.add(statusMessage)
        })
      })

      val set = profileAlertStatusAccumulator.value
      logger.info("Batch complete.  Current state of profilers, profiles, and alerts follows with " + set.size + " messages")

      set.foreach((statusMessage) => {
        logger.info(statusMessage)
      })

      //Remove status messages since they've already been written out.
      profileAlertStatusAccumulator.setValue(profileAlertStatusAccumulator.zero)
    })
  }

  def createEmptyInitialState(sparkContext:SparkContext,
      fieldToProfilerMap:Map[Field, Set[ActivityProfiler[String]]])
      :RDD[(Field, FieldProfileState)] = {
    logger.info("Creating empty initial state.")

    val initiateState = fieldToProfilerMap.map(pair => {
      val field = pair._1
      val profilerSet = pair._2
      val profilerToNewProfileMappings:Map[String, ActivityProfile[String]] = profilerSet.map((profiler: ActivityProfiler[String]) => {
        val profilerKey = profiler.key
        (profilerKey, new ActivityProfile(Set.empty[String]))
      }).toMap

      (field, new FieldProfileState(profilerToNewProfileMappings)) //Map the field to the map of profilers and profiles for use on in the next batch
    }).toSeq

    sparkContext.parallelize(initiateState)
  }

  def profileCloudTrailEvents(arrivingEvents:DStream[String],
                              sparkContext:SparkContext,
                              initialState:RDD[(Field, FieldProfileState)],
                              fieldToProfilerMap:Map[Field, Set[ActivityProfiler[String]]]):DStream[(Field, FieldProfileState)] = {
    logger.info("Configuring transforms for streaming CloudTrail data")
    val targetFieldsAndValues:DStream[(Field, String)] = getTargetFieldsAndValues(arrivingEvents, fieldToProfilerMap)

    val hashPartitioner:Partitioner = new HashPartitioner(sparkContext.defaultParallelism)

    //Create partially-applied function for updatingStateByKey
    def updateFunction:(Iterator[(Field, Seq[String], Option[FieldProfileState])]
      => Iterator[(Field, FieldProfileState)]) = profileAndUpdateState(_, fieldToProfilerMap)

    val stateDStream = targetFieldsAndValues.updateStateByKey(updateFunction, hashPartitioner, true, initialState)
    stateDStream
  }

  /*Map from CloudTrail events to pairs of (target fields, list of values)
  * Our input is individual CloudTrail events as JSON strings.  We need to convert them to JSON trees and
  * then find all the values for only the fields we're interested in.  We then want to de-duplicate values across
  * different CloudTrail events.  We want the results as a map between the fields we're interested in and the
  * values for those fields.*/
  def getTargetFieldsAndValues(arrivingEvents:DStream[String],
                               fieldToProfilerMap:Map[Field, Set[ActivityProfiler[String]]]):DStream[(Field, String)] = {
    if (fieldToProfilerMap.isEmpty) {
      logger.warn("Field to profile map is empty!")
    }

    val targetFieldsAndValues:DStream[(Field, String)] = arrivingEvents.flatMap((eventAsJsonString:String) => {
      if (eventAsJsonString == null || eventAsJsonString.equals("")) {
        logger.warn("An empty string was received instead of a CloudTrail event.")
      }
      val fieldValuePairs:Set[(Field, String)] = fieldToProfilerMap.keySet.flatMap((fieldToProfile: Field) => {
        val fieldValues = findValuesForCloudTrailField(fieldToProfile.name, eventAsJsonString)

        if (fieldValues.isEmpty) {
          logger.warn("No values were found for " + fieldToProfile.name + " in " + eventAsJsonString)
        }
        else {
          logger.info("New values for " + fieldToProfile.name + " are " + fieldValues)
        }

        //Pair each value up with its field name (and metadata).  This will be collapsed later.
        fieldValues.map((fieldValue:String) => {
          (fieldToProfile, fieldValue)
        })
      })
      fieldValuePairs
    })

    targetFieldsAndValues
  }

  def profileAndUpdateState(targetFieldsAndValues:Iterator[(Field, Seq[String], Option[FieldProfileState])],
      fieldToProfilerMap:Map[Field, Set[ActivityProfiler[String]]])
      :Iterator[(Field, FieldProfileState)] = {
    logger.info("Profiling and updating state for all fields")

    if (targetFieldsAndValues.isEmpty) {
      logger.error("No target fields and values found!")
    }
    else {
      logger.debug("Target fields and values found.")
    }

    val newState = targetFieldsAndValues.map(triple => {
      val field = triple._1
      val newValues = triple._2
      val previousState = triple._3
      profileAndUpdateStateForField(field, newValues, previousState, fieldToProfilerMap)
    })

    newState
  }

  def profileAndUpdateStateForField(field:Field,
      newValues:Seq[String],
      previousStateOption:Option[FieldProfileState],
      fieldToProfilerMap:Map[Field, Set[ActivityProfiler[String]]])
      :(Field, FieldProfileState) = {
    logger.info("Profiling and saving new state for " + field.name)
    logger.info("Previous state is " + previousStateOption)
    val newActivity = newValues.toSet //Remove duplicates

    val profilerSetOption = fieldToProfilerMap.get(field)
    if (profilerSetOption.isDefined) {
      val profilerSet = profilerSetOption.get

      val newState:FieldProfileState = if (previousStateOption.isDefined) {
        val previousState = previousStateOption.get

        //Iterate over all the profilers for this field and compare against previous profiles
        val newMappings = compareAgainstPreviousProfilesAndUpdateProfiles(profilerSet, previousState,
          newActivity, field)
        logger.info("New mappings: " + newMappings)
        previousState.updateProfilerStateMappings(newMappings)
      }
      else {
        logger.warn("No previous state found for comparisons.  We will create state from the values in the current run.")
        val newMappings = profilerSet.map((profiler: ActivityProfiler[String]) => {
          val profilerKey = profiler.key
          (profilerKey, new ActivityProfile(newActivity))
        }).toMap

        new FieldProfileState(newMappings)
      }

      logger.info("New state is " + newState)
      (field, newState) //Map the field to the map of profilers and profiles for use on in the next batch
    }
    else {
      logger.error("Could not find a profiler for the following field: " + field.name)
      (field, new FieldProfileState(Map.empty)) //Return empty state since there's no profiler for which to store state
    }
  }

  def compareAgainstPreviousProfilesAndUpdateProfiles(profilerSet:Set[ActivityProfiler[String]],
                                                      previousState:FieldProfileState,
                                                      newActivity:Set[String],
                                                      field:Field):Map[String, ActivityProfile[String]] = {
    logger.info("Comparing new activity against past state for " + field.name + " and updating profiles")

    if (profilerSet.isEmpty) {
      logger.warn("Set of profilers is empty.")
    }

    if (previousState.mappings.isEmpty) {
      logger.warn("No previous state found.")
    }

    if (newActivity.isEmpty) {
      logger.warn("No new activity found.")
    }

    //Iterate over all the profilers for this field and compare against previous profiles
    val profilerToNewProfileMappings = profilerSet.map((profiler: ActivityProfiler[String]) => {
      val profilerKey = profiler.key
      val previousProfileOption = previousState.mappings.get(profilerKey)
      if (previousProfileOption.isDefined) {
        val previousProfile:ActivityProfile[String] = previousProfileOption.get
        logger.info("Old profile was " + previousProfile)
        val newProfile:ActivityProfile[String] = profiler.compareNewActivity(newActivity, previousProfile)
        logger.info("New profile is " + newProfile)
        (profilerKey, newProfile) //Map the profiler to the profile so we can retrieve it later.
      }
      else {
        logger.error("Could not find a previous profile for the following field:" + field.name)
        (profilerKey, new ActivityProfile(newActivity)) //Map the profiler to the profile so we can retrieve it later.
      }
    }).toMap

    if (profilerToNewProfileMappings.isEmpty) {
      logger.warn("Profiler to new profile mappings is empty!")
    }

    profilerToNewProfileMappings
  }

  def findValuesForCloudTrailField(fieldName:String, cloudTrailEvent:String):Set[String] = {
    val tree = ObjectMapperSingleton.mapper.readTree(cloudTrailEvent)
    tree.findValuesAsText(fieldName).toSet
  }
}
