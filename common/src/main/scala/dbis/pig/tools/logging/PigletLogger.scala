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
object PigletLogger  {
  
  protected[logging] def apply(underlying: Underlying): PigletLogger = new PigletLogger(Some(underlying))

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

final class PigletLogger private[logging](private[this] val underlying: Option[Underlying]) extends java.io.Serializable {
  
  setLevel(PigletLogger.level)
  
  def setLevel(level: LogLevel): Unit = {
    if(underlying.isDefined) {
    	underlying.get.setLevel(PigletLogger.underlyingLevel(level))
    	PigletLogger.level = level
    }
  }
  
  def getLevel: LogLevel = if(underlying.isDefined) PigletLogger.level(underlying.get.getLevel) else LogLevel.OFF 
  
  def error(msg: String): Unit = if(underlying.isDefined) if(underlying.get.isErrorEnabled()) underlying.get.error(msg) 
  
  def error(msg: String, cause: Throwable): Unit = if(underlying.isDefined) if(underlying.get.isErrorEnabled())  underlying.get.error(msg, cause)

  def warn(msg: String): Unit = if(underlying.isDefined) if(underlying.get.isWarnEnabled()) underlying.get.warn(msg)
  
  def warn(msg: String, cause: Throwable): Unit = if(underlying.isDefined) if(underlying.get.isWarnEnabled())  underlying.get.warn(msg, cause)
  
  def info(msg: String): Unit = if(underlying.isDefined) if(underlying.get.isInfoEnabled()) underlying.get.info(msg)
  
  def info(msg: String, cause: Throwable): Unit = if(underlying.isDefined) if(underlying.get.isInfoEnabled())  underlying.get.info(msg, cause)
  
  def debug(msg: String): Unit = if(underlying.isDefined) if(underlying.get.isDebugEnabled()) underlying.get.debug(msg)
  
  def debug(msg: String, cause: Throwable): Unit = if(underlying.isDefined) if(underlying.get.isDebugEnabled())  underlying.get.debug(msg, cause)
  
}