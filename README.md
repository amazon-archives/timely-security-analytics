# timely-security-analytics Overview
This repo contains demo code for the Timely Security Analytics and Analysis presentation at the Amazon Web Services 2015 Re:Invent conference.  We are open sourcing this project so that others may learn from it and potentially build on it.  It really contains three interesting, independent units:
* CloudTrailToSQL - See the comments at the top of the file which contain instructions to convert your AWS CloudTrail logs to a Spark Data Frame which you can then query with SQL
* reinvent.securityanalytics.CloudTrailProfileAnalyzer is a Spark Streaming application which will read your CloudTrail logs (new and historical), build profiles based on those logs, and alert you when activity deviates from those profiles.
* S3LogsToSQL is alpha code to convert your Amazon S3 logs into tables which can be queried.

This page will be updated with a link to the video of the talk, when it is available.

#CloudTrailToSQL
Do you want to run SQL queries over your CloudTrail logs?  Well, you're in the right place.  This code is written so you can cut-and-paste it into a spark-shell and then start running SQL queries (hence why there is no package in this code and I've used as few libraries as possible).  All the libraries it requires are already included in the build of Spark 1.4.1 that's available through Amazon's Elastic MapReduce (EMR).  For more information, see https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/

##How to use it
1. Provision an EMR cluster with Spark 1.4.1 and an IAM role that has CloudTrail and S3 describe and read permissions.
2. SSH to that cluster and run that spark-shell, e.g. spark-shell --master yarn-client --num-executors 40 --conf spark.executor.cores=2
3. Cut and paste the contents of CloudTrailToSQL.scala (found in this package) into your Spark Shell (once the scala> prompt is available)
4. Run the following commands:
```
var cloudtrail = CloudTrailToSQL.createTable(sc, sqlContext) //creates and registers the Spark SQL table
CloudTrailToSQL.runSampleQuery(sqlContext) //runs a sample query
```
Note that these commands will take some time to run as they load your CloudTrail data from S3 and store it in-memory on the Spark cluster.  Run the sample query again and you'll see the speed up that the in-memory caching provides.
5. Run any SQL query you want over the data, e.g.
```
sqlContext.sql("select distinct eventSource, eventName, userIdentity.principalId from cloudtrail where userIdentity.principalId = userIdentity.accountId").show(99999) //Find services and APIs called with root credentials
```
6.  You can create a Hive table (that will persist after your program exits) by running
```
var cloudtrail = CloudTrailToSQL.createHiveTable(sc, sqlContext)
```
##Additional uses
You can configure and invoke geoIP lookup functions using code like that below.  To do this, you will need a copy of the Maxmind GeoIP database.  See the Dependencies section of this documentation.
```
import reinvent.securityanalytics.utilities.Configuration
import reinvent.securityanalytics.GeoIPLookup
var config = new Configuration("<YOUR BUCKET>", "config/reinventConfig.properties")
var geoIP = new GeoIPLookup(config)
geoIP.registerUDFs(sqlContext) //Registers UDFs that you can use for lookups.
sqlContext.sql("select distinct sourceIpAddress, city(sourceIpAddress), country(sourceIpAddress) from cloudtrail").collect.foreach(println)
```
#CloudTrailProfileAnalyzer
##How to use it
1. Fill out a config file and load it in S3
2. (Optional) License Maxmind's GeoIP DB to get use of the GeoIP functionality
3. Start an EMR cluster with Spark 1.4.1
4. Compile the code with "mvn package"
5. Upload the fat jar (e.g., cloudtrailanalysisdemo-1.0-SNAPSHOT-jar-with-dependencies.jar) to your EMR cluster
6. Submit it using spark-submit.  See resources/startStreaming.sh for an example.  Make sure to pass the bucket and key that points to your config file.
7.  Look for alerts via the subscriptions set up on your SNS topic.

##Future work
* The current code builds profiles based on deduplicated historical data, which doesn't allow for easy frequency analysis.  Passing the data, with duplicates, would allow for more meaningful analysis.
* The current code operates on a per-field basis and should be improved to allow profilers to operate on the full activity.  For example, its hard prevent duplicate alarms on Tor usage since Tor usage, by definition, comes from different exit nodes.  It would make more sense to ignore duplicates based on the actor rather than the source IP address.

#Dependencies
This code has the key dependencies described below.  For a full list, including versions, please see the pom.xml file included in the repo.
* Apache Spark is our core processing engine.
* AWS Java SDK is how we communicate with AWS.
* Scala version 2.10 is the language in which this code is written.
* The Maxmind [GeoIP database](http://dev.maxmind.com/geoip/geoip2/downloadable/). 
* The Maxmind [GeoIP2 Java library](https://github.com/maxmind/GeoIP2-java).
