package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.Cross
import dbis.piglet.op.Pipe

/**
  * Created by kai on 12.12.16.
  */
class CrossEmitter extends CodeEmitter[Cross] {

  // the template for final code
  override def template: String = """
    |val <out> = <code>.map{case l =>
    |  val res = convert<class>(l)
    |  <if(profiling)>
    |    PerfMonitor.sampleSize(t,"<lineage>", accum, randFactor)
    |  <endif>
    |  res
    |\}
    """.stripMargin

  // code template to create all cartesian products one after he other
  val t = "<rel1>.cartesian(<rel2>).map{ case (v,w)  => (<fields>) \\}"
    
  private def makeFieldString(p: Pipe, prefix: String, offset: Int = 0) = p.inputSchema.get.fields.indices.map(i => s"$prefix._${i+offset}").mkString(",")
    
  override def code(ctx: CodeGenContext, op: Cross): String = {
    val rels = op.inputs
    
    /* first we construct the resulting fields of the first product
     * the offste parameter is left to 0 because the field name numbering in the schema classes starts at 0
     */
    var fieldList = s"${makeFieldString(rels.head, "v")},${makeFieldString(rels(1), "w")}"
    
    // create the code for the first cross product
    var code = CodeEmitter.render(t, Map(
        "rel1" -> rels.head.name, 
        "rel2" -> rels(1).name, 
        "fields" -> fieldList))    
    
        
    // if we have mor than two input relation, we need to add the subsequent cross operations one after the other    
    for(i <- 2 until op.inputs.size) {
    
      // count how many fields have been created by the previous CROSS's
      val numFields = (0 until i).map(j => rels(j).inputSchema.get.fields.length).sum
      
      // the resulting fields consist for all fields of the previous results + the fields of the current input relation
      fieldList = (1 to numFields).map(j => s"v._$j").mkString(",") + "," + makeFieldString(rels(i), "w", 0)
      
      // produce code 
      code = CodeEmitter.render(t, Map(
          "rel1" -> code, 
          "rel2" -> op.inputs(i).name, 
          "fields" -> fieldList))  
      
    }
    

    val className = op.schema match {
      case Some(s) => ScalaEmitter.schemaClassName(s.className)
      case None => ScalaEmitter.schemaClassName(op.outPipeName)
    }
    
    // use the sequence of cartesian products in the final template
    render(Map(
      "out" -> op.outPipeName,
      "class" -> className,
      "code" -> code,
      "lineage" -> op.lineageSignature))
  }

}

object CrossEmitter {
  lazy val instance = new CrossEmitter
}
