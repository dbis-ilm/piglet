package dbis.piglet.api

import dbis.piglet.plan.rewriting.Rewriter._
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.parser.PigParser
import dbis.piglet.backends.BackendManager
import dbis.piglet.tools.Conf
import dbis.piglet.codegen.CodeGenerator
import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.schema.Schema

object PigletInterpreterAPI extends PigletLogging {

  /**
   * Create Scala code for the given backend from the source string.
   * This method is provided mainly for Zeppelin.
   *
   * @param source the Piglet script
   * @param backend the backend used to compile and execute
   * @return the generated Scala code
   */
  def createCodeFromInput(source: String, backend: String): String = {
	  import scala.collection.JavaConverters._

	  Schema.init()
    var plan = new DataflowPlan(PigParser.parseScript(source))

    if (!plan.checkConnectivity) {
      logger.error(s"dataflow plan not connected")
      return ""
    }

    logger.debug(s"successfully created dataflow plan")
    plan = processPlan(plan)

    // compile it into Scala code for Spark
    val generatorClass = Conf.backendGenerator(backend)
    val extension = Conf.backendExtension(backend)
    val backendConf = BackendManager.init(backend)
//      BackendManager.backend = backendConf
    val templateFile = backendConf.templateFile
    val args = Array(templateFile).asInstanceOf[Array[AnyRef]]
    val compiler = Class.forName(generatorClass).getConstructors()(0).newInstance(args: _*).asInstanceOf[CodeGenerator]

    // 5. generate the Scala code
    val code = compiler.generate("blubs", plan, profiling = None, forREPL = true)
    logger.debug("successfully generated scala program")
    code
  }
}
