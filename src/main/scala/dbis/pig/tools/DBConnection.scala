package dbis.pig.tools

import scalikejdbc._
import com.typesafe.scalalogging.LazyLogging

case class ConnectionSetting(val driver: String, val url: String, val user: String, val pw: String)

object DBConnection extends LazyLogging {
  
  
  def init(setting: ConnectionSetting) {
    logger.debug(s"loading database driver: ${setting.driver}")
    Class.forName(setting.driver)
    
    logger.debug(s"connecting to DB at ${setting.url} as user ${setting.user}")
    ConnectionPool.singleton(setting.url, setting.user, setting.pw)

  } 
  
  def exit() {    
    // wipes out ConnectionPool
    ConnectionPool.closeAll()
    logger.debug("closed DB connection pool")
  }


  
}
