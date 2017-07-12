package dbis.piglet.codegen

import java.net.URI

import dbis.piglet.op.PigOperator
import dbis.piglet.tools.logging.PigletLogging
import org.clapper.scalasti.ST
import org.stringtemplate.v4.AutoIndentWriter
import org.stringtemplate.v4.misc.ErrorBuffer

case class CodeGenException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
}

object CodeGenException {
  def apply(msg: String) = new CodeGenException(msg)
}


abstract class CodeEmitter[O <: PigOperator] extends PigletLogging {
  def template: String

  def render: String = CodeEmitter.render(template, Map[String, Any]())

  /**
   * Invoke a given string template with a map of key-value pairs used for replacing
   * the keys in the template by the string values.
   *
   * @param params the map of key-value pairs
   * @return the text from the template
   */
  def render(params: Map[String, Any]): String = CodeEmitter.render(template, params)

  def helper(ctx: CodeGenContext, node: O): String = ""

  def code(ctx: CodeGenContext, node: O): String

  def beforeCode(ctx: CodeGenContext, node: O): String = ""

  def afterCode(ctx: CodeGenContext, node: O): String = ""

}

object CodeEmitter {

  protected[codegen] var profiling: Boolean = false

  val sw = new java.io.StringWriter

  /**
   * Invoke a given string template with a map of key-value pairs used for replacing
   * the keys in the template by the string values.
   *
   * @param params the map of key-value pairs
   * @return the text from the template
   */
  def render(template: String, params: Map[String, Any]): String = {

    var st = ST(template)
    params.foreach { case (name, value) => st = st.add(name, value) }

    st = st.add("profiling", profiling)

//    st.render() match {
//      case Success(code) => code
//      case Failure(e) => println(e.getMessage); ""//throw e//CodeGenException(s"Could not render template $template with params ${theParams.mkString("\n")}", e)
//    }

    // Ugly version to suppress warnings such as "attribute isn't defined":
    sw.getBuffer.setLength(0)
    st.nativeTemplate.write(new AutoIndentWriter(sw), new ErrorBuffer())
    sw.flush()
    sw.toString
  }

}
