package dbis.pig.tools.logging

import ch.qos.logback.classic.{Logger => Underlying}
import ch.qos.logback.classic.{Level => UnderlyingLevel}


object LogLevel extends Enumeration {
  type LogLevel = Value
  val OFF, ERROR, WARN, INFO, DEBUG = Value
}

import LogLevel._

/**
 * The Piglet logger. 
 * 
 * This is an abstraction for an exchangeable underlying logging framework.
 * 
 * Inspired by Scala-Logging framework: https://github.com/typesafehub/scala-logging/blob/master/src/main/scala/com/typesafe/scalalogging/Logger.scala
 * 
 * The difference is that we allow to set the log level programmatically 
 */
object PigletLogger {
  
  protected[logging] def apply(underlying: Underlying): PigletLogger = new PigletLogger(underlying)

  private[logging] var level: LogLevel = WARN
  
  private[logging] def underlyingLevel(lvl: LogLevel): UnderlyingLevel = lvl match {
    case OFF => UnderlyingLevel.OFF
    case ERROR => UnderlyingLevel.ERROR
    case WARN => UnderlyingLevel.WARN
    case INFO => UnderlyingLevel.INFO
    case DEBUG => UnderlyingLevel.DEBUG
  }
  
  private[logging] def level(lvl: UnderlyingLevel): LogLevel = lvl match {
    case UnderlyingLevel.OFF => OFF
    case UnderlyingLevel.ERROR => ERROR
    case UnderlyingLevel.WARN => WARN
    case UnderlyingLevel.INFO => INFO
    case UnderlyingLevel.DEBUG => DEBUG
  }
  
}

final class PigletLogger private(private[this] val underlying: Underlying) {
  
  setLevel(PigletLogger.level)
  
  def setLevel(level: LogLevel): Unit = {
    underlying.setLevel(PigletLogger.underlyingLevel(level))
    PigletLogger.level = level
  }
  
  def getLevel: LogLevel = PigletLogger.level(underlying.getLevel)
  
  def error(msg: String): Unit = if(underlying.isErrorEnabled()) underlying.error(msg)
  
  def error(msg: String, cause: Throwable): Unit = if(underlying.isErrorEnabled())  underlying.error(msg, cause)

  def warn(msg: String): Unit = if(underlying.isWarnEnabled()) underlying.warn(msg)
  
  def warn(msg: String, cause: Throwable): Unit = if(underlying.isWarnEnabled())  underlying.warn(msg, cause)
  
  def info(msg: String): Unit = if(underlying.isInfoEnabled()) underlying.info(msg)
  
  def info(msg: String, cause: Throwable): Unit = if(underlying.isInfoEnabled())  underlying.info(msg, cause)
  
  def debug(msg: String): Unit = if(underlying.isDebugEnabled()) underlying.debug(msg)
  
  def debug(msg: String, cause: Throwable): Unit = if(underlying.isDebugEnabled())  underlying.debug(msg, cause)
  
}