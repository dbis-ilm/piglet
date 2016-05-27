package dbis.pig.tools

import scalikejdbc._
import dbis.pig.tools.logging.PigletLogging
import java.net.URI

case class ConnectionSetting(val url: URI, val user: String, val pw: String, val driver: String = "org.postgresql.Driver")

object ConnectionSetting extends PigletLogging {
  
  /**
   * Creating a connection settings object from connection string
   * of the form schema://host:port/dbname?user=username&pw=password
   */
  def apply(connectionString: URI): ConnectionSetting = {
    logger.debug(s"connection string $connectionString")
    
    var user = ""
    var pass = ""
    
    val a = connectionString.getQuery
      .split("&")
      .map { p => val o = p.split("="); o(0) -> o(1) }
      .foreach { case (k,v) => k match {
        case "user" => user = v
        case "pw" => pass = v
        case _ => throw new IllegalArgumentException(s"unknown connection string parameter: $k")
        }
      }
    
    val uriString = connectionString.toString()
    val url = s"${if(uriString.startsWith("jdbc:")) "" else "jdbc:" }${uriString.substring(0,uriString.indexOf("?"))}"
    
    ConnectionSetting(new URI(url), user, pass)
  }
  
}


object DBConnection extends PigletLogging {
  
  
  def init(setting: ConnectionSetting) {
    logger.debug(s"settings: $setting")
    logger.debug(s"loading database driver: ${setting.driver}")
//    Class.forName(setting.driver)
    
    val poolSettings = ConnectionPoolSettings(
        driverName = setting.driver,
        connectionTimeoutMillis=10000,
        initialSize = 2,
        maxSize=10
        )
    
    logger.debug(s"connecting to DB at ${setting.url} as user ${setting.user}")
    ConnectionPool.singleton(setting.url.toString(),setting.user,setting.pw, poolSettings)
  } 
  
  def exit() {    
    // wipes out ConnectionPool
    ConnectionPool.closeAll()
    logger.debug("closed DB connection pool")
  }


  
}
