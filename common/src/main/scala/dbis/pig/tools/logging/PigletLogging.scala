package dbis.pig.tools.logging

import org.slf4j.LoggerFactory

trait PigletLogging {
  
  protected val logger: PigletLogger = PigletLogger(LoggerFactory.getLogger(getClass.getName).asInstanceOf[ch.qos.logback.classic.Logger])
  
}