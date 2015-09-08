package dbis.pfabric.deploy.util

import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import com.typesafe.scalalogging.LazyLogging
import scala.collection.JavaConverters._
import org.apache.hadoop.yarn.util.{ Apps, ConverterUtils }
import java.io._
/**
 * @author hduser
 */
object YarnUtils extends LazyLogging {
  
   /**
   * Prepared the command which is needed to run the ApplicationMaster class. This class is provided with some
   * arguments to configure the logs
   */
  def prepareCommand(taskName: String): String = {
    // Set java executable command
    logger.info("Setting up app master command")
    val command = s"""
        | ${Environment.JAVA_HOME.$$()}/bin/java
        | dbis.pfabric.deploy.yarn.appmaster.AppMaster
        | 1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/${taskName}.${ApplicationConstants.STDOUT} 
        | 2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/${taskName}.${ApplicationConstants.STDERR}
        """.stripMargin.replaceAll("\n", " ")
    logger.info("Completed setting up app master command " + command)
    command
  }

  /**
   * prepare the OS environmental variables
   */
  def prepareOSEnv(config: YarnConfiguration) = {
    var env = Map[String, String]()
    val classPath = config.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
    for (c <- classPath) {
      Apps.addToEnvironment(env.asJava, Environment.CLASSPATH.name(),
        c.trim())
    }
    Apps.addToEnvironment(env.asJava,
      Environment.CLASSPATH.name(),
      Environment.PWD.$() + File.separator + "*")
    logger.info("CLASSPATH = " + env.mkString)
    env
  }

}