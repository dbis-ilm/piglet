package dbis.pig.backends.mapreduce

import dbis.pig.backends.BackendConf
import dbis.pig.backends.PigletBackend
import java.nio.file.Path
import org.apache.pig.PigServer
import org.apache.pig.ExecType
import org.apache.pig.tools.pigstats.PigProgressNotificationListener
import org.apache.pig.PigRunner

/**
 * @author hage
 */
class PigRun extends PigletBackend with BackendConf {

  override def execute(master: String, className: String, jarFile: Path) = ???
  
  override def executeRaw(program: Path, master: String) {
    
    val stats = PigRunner.run(Array("-x", execType(master), program.toAbsolutePath().toString() ), null)
    
  }
  
  /**
   * Get the name of this backend
   * 
   * @return Returns the name of this backend
   */
  override def name: String = "MapReduce - Pig"
  
  /**
   * Get the path to the runner class that implements the PigletBackend interface
   */
  override def runnerClass: PigletBackend = this
  
  override def templateFile = null
  
  override def raw = true
  
  private def execType(master: String) = if(master.startsWith("local")) "local" else "mapreduce"
//  implicit private def execType(master: String) = if(master.toLowerCase().startsWith("local")) ExecType.LOCAL else ExecType.MAPREDUCE
}