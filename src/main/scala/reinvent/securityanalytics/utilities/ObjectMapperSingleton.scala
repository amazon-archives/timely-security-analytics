package reinvent.securityanalytics.utilities

import com.fasterxml.jackson.databind.ObjectMapper

//Make sure we load the ObjectMapper once and only once per executor
object ObjectMapperSingleton {
  private var _mapper:Option[ObjectMapper] = None
  def mapper:ObjectMapper = {
    _mapper match {
      case Some(wrapped) => { wrapped }
      case None => {
        _mapper = Some(new ObjectMapper())
        mapper
      }
    }
  }
}
