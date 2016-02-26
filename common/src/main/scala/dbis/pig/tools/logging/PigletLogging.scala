package dbis.pig.tools.logging

import java.io.PrintStream

import org.slf4j.LoggerFactory

trait PigletLogging {

  /*
   * This ugly hack is used to suppress the annoying warning of multiple bindings in slf4j.
   */
  val filteredErr = new PrintStream(System.err) {
    override def println(l: String) = if (!l.startsWith("SLF4J") && !l.startsWith("[INFO ] [EtmMonitor]")) super.println(l)
  }
  System.setErr(filteredErr)
  
  val filteredOut = new PrintStream(System.out) {
    override def println(l: String) = if (!l.startsWith("SLF4J") && !l.startsWith("[INFO ] [EtmMonitor]")) super.println(l)
  }
  System.setOut(filteredOut)
  
  


  protected val logger: PigletLogger = PigletLogger(LoggerFactory.getLogger(getClass.getName).asInstanceOf[ch.qos.logback.classic.Logger])
  
}