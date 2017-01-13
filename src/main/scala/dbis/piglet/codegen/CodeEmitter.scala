package dbis.piglet.codegen

import org.clapper.scalasti.ST
import org.stringtemplate.v4.AutoIndentWriter
import org.stringtemplate.v4.misc.ErrorBuffer

import dbis.piglet.op.PigOperator

case class CodeGenException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
}

object CodeGenException {
  def apply(msg: String) = new CodeGenException(msg)
}


abstract class CodeEmitter[O <: PigOperator] {
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
  val sw = new java.io.StringWriter

  /**
   * Invoke a given string template with a map of key-value pairs used for replacing
   * the keys in the template by the string values.
   *
   * @param params the map of key-value pairs
   * @return the text from the template
   */
  def render(template: String, params: Map[String, Any]): String = {

    val st = ST(template)
    if (params.nonEmpty) {
      params.foreach {
        attr => st.add(attr._1, attr._2)
      }
    }
    //st.render()
    // Ugly version to suppress warnings such as "attribute isn't defined":
    sw.getBuffer.setLength(0)
    st.nativeTemplate.write(new AutoIndentWriter(sw), new ErrorBuffer())
    sw.flush()
    sw.toString()
  }

}
