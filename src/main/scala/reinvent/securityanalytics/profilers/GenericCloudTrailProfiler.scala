package reinvent.securityanalytics.profilers

import reinvent.securityanalytics.state.Field
import reinvent.securityanalytics.utilities.Configuration

/**A GenericCloudTrailProfiler will track new values for a given field in CloudTrail events. */
class GenericCloudTrailProfiler[T] (field:Field, config:Configuration) extends ActivityProfiler[T](field.name, config)
