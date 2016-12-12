package dbis.piglet.codegen

import org.clapper.scalasti.ST
import org.stringtemplate.v4.NoIndentWriter
import org.stringtemplate.v4.misc.ErrorBuffer

import dbis.piglet.op.PigOperator

case class CodeGenException(msg: String) extends Exception(msg)

trait CodeEmitter {
  def template: String

  def render: String = CodeEmitter.render(template, Map[String, Any]())

  /**
    * Invoke a given string template with a map of key-value pairs used for replacing
    * the keys in the template by the string values.
    *
    * @param params the map of key-value pairs
    * @return the text from the template
    */
  def render(params: Map[String,Any]): String = CodeEmitter.render(template, params)

  def helper(ctx: CodeGenContext, node: PigOperator): String = ""

  def code(ctx: CodeGenContext, node: PigOperator): String

  def beforeCode(ctx: CodeGenContext, node: PigOperator): String = ""

  def afterCode(ctx: CodeGenContext, node: PigOperator): String = ""

}

object CodeEmitter {
   /**
    * Invoke a given string template with a map of key-value pairs used for replacing
    * the keys in the template by the string values.
    *
    * @param params the map of key-value pairs
    * @return the text from the template
    */
  def render(template: String, params: Map[String,Any]): String = {
    val st = ST(template)
    if (params.nonEmpty) {
      params.foreach {
        attr => st.add(attr._1, attr._2)
      }
    }
    //st.render()
    // Ugly version to suppress warnings such as "attribute isn't defined":
    val out = new java.io.StringWriter
    st.nativeTemplate.write(new NoIndentWriter(out), new ErrorBuffer())
    out.flush()
    out.toString()
  }


}