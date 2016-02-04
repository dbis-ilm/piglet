package dbis.pig.tools.logging

import java.io.PrintStream

import org.slf4j.LoggerFactory

trait PigletLogging {

  /*
   * This ugly hack is used to suppress the annoying warning of multiple bindings in slf4j.
   */
  val filterOut = new PrintStream(System.err) {
    override def println(l: String) = if (! l.startsWith("SLF4J")) super.println(l)
  }
  System.setErr(filterOut)


  protected val logger: PigletLogger = PigletLogger(LoggerFactory.getLogger(getClass.getName).asInstanceOf[ch.qos.logback.classic.Logger])
  
}