package reinvent.securityanalytics.state

class Field(val name:String) extends Serializable {
  def ignore(value:String):Boolean = false
}

case object SOURCE_IP_ADDRESS extends Field("sourceIPAddress")
case object AWS_ACCESS_KEY_ID extends Field("accessKeyId")
case object PRINCIPAL_ID extends Field("principalId")
case object PRINCIPAL_ARN extends Field("arn")
