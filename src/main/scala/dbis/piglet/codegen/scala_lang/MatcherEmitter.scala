package dbis.piglet.codegen.scala_lang


import scala.collection.mutable.ListBuffer
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op._

/**
  * Created by kai on 03.12.16.
  */
class MatcherEmitter extends CodeEmitter[Matcher] {
  override def template: String = """val <out> = <in>.matchNFA(<in>NFA.createNFA, <mode>)""".stripMargin
  def helperTemplate: String = """object <out>NFA {
                                 |        <filters, predcs:{ f, pred |def filter<f> (t: <class>, rvalues: NFAStructure[<class>]) : Boolean = <pred><\n>}>
                                 |        def createNFA = {
                                 |            val <out>OurNFA: NFAController[<class>] = new NFAController()<\n>
                                 |            <states, types:{ s, t |val <s>State = <out>OurNFA.createAndGet<t>State("<s>")<\n>}>
                                 |            <filters:{ f |val <f>Edge = <out>OurNFA.createAndGetForwardState(filter<f>)<\n>}>
                                 |            <filters, tran_states, tran_next_states:{ f, s, n |<out>OurNFA.createForwardTransition(<s>State, <f>Edge, <n>State)<\n>}>
                                 |            <out>OurNFA
                                 |        }
                                 |    }""".stripMargin

  override def helper(ctx: CodeGenContext, op: Matcher): String = {
    val filters = op.events.complex.map { f => f.simplePattern.asInstanceOf[SimplePattern].name }
    val predicates = op.events.complex.map {
      f => ScalaEmitter.emitPredicate(CodeGenContext(ctx, Map("schema" -> op.schema, "events" -> Some(op.events))), f.predicate)
    }
    val hasSchema = op.schema.isDefined
    val schemaClass = if (!hasSchema) {
      "Record"
    } else {
      ScalaEmitter.schemaClassName(op.schema.get.className)
    }
    var states: ListBuffer[String] = new ListBuffer()
    states += "Start"
    emitStates(op.pattern, states)
    val transStates = (states - states.last).toList
    val tranNextStates = states.tail.toList
    val types = states.zipWithIndex.map { case (x, i) =>
      if (i == states.length - 1) "Final"
      else "Normal"
    }
    CodeEmitter.render(helperTemplate,
      Map("out" -> op.in.name,
        "init" -> "",
        "class" -> schemaClass,
        "filters" -> filters,
        "predcs" -> predicates,
        "states" -> states.toList,
        "tran_states" -> transStates,
        "tran_next_states" -> tranNextStates,
        "types" -> types.toList))
  }

  override def code(ctx: CodeGenContext, op: Matcher): String = {
    render(Map("out" -> op.outPipeName,
      "in" -> op.in.name,
      "mode" -> (op.mode.toLowerCase() match {
        case "skip_till_any_match" => "AllMatches"
        case "first_match" => "FirstMatch"
        case "recent_match" => "RecentMatches"
        case "cognitive_match" => "CognitiveMatches"
        case _ => "NextMatches"
      })))
  }


   private def emitStates (pattern: Pattern, states: ListBuffer[String] ) {
    pattern match  {
      case SimplePattern(name) => states += name
      case NegPattern(pattern) => emitStates(pattern, states)
      case SeqPattern(patterns) => patterns.foreach { p => emitStates(p, states) }
      case DisjPattern(patterns) => patterns.foreach { p => emitStates(p, states) }
      case ConjPattern(patterns) => patterns.foreach { p => emitStates(p, states) }
    }
  }
}

object MatcherEmitter {
  lazy val instance = new MatcherEmitter
}