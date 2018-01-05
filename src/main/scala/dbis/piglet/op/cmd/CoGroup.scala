package dbis.piglet.op.cmd

import dbis.piglet.expr.NamedField
import dbis.piglet.op.{GroupingExpression, PigOperator, Pipe}
import dbis.piglet.schema._

case class CoGroup(private val out: Pipe,
                   private val ins: List[Pipe],
                   groupExprs: List[GroupingExpression]) extends PigOperator(List(out), ins) {


  override def constructSchema: Option[Schema] = {
    // tuple(group: typeOfGroupingExpr, in:bag(inputSchema))
    val inSchemas = inputs.map(_.producer.schema)


    val inputTypes = inSchemas.map {
      case Some(s) => s.element.valueType
      case None => TupleType(Array(Field("", Types.ByteArrayType)))
    }

//    val groupingTypes = groupExprs.zip(inSchemas).map { case (e,s) => e.resultType(s) } //(_.resultType(inputSchema))
    val groupingType = groupExprs.head.resultType(inSchemas.head)

    // the group field gets the original grouping expression as lineage, e.g. rel.column
    val groupField = Field("group", groupingType, List(s"$inPipeName.${groupExprs.head.keyList.mkString}"))

//    val aggField = Field(inputs.head.name, BagType(inputTypes))

    val aggFields = inputs.zip(inputTypes).map { case (in, inType) => Field(in.name, BagType(inType)) }


    val fields = (groupField :: aggFields).toArray


    schema = Some(Schema(fields))
    schema
  }

  override def checkSchemaConformance: Boolean = inputs.map(_.producer.schema).zip(groupExprs).forall { case (inS, expr) =>
    inS match {
      case Some(s) =>
        // if we know the schema we check all named fields
        ! expr.keyList.filter(_.isInstanceOf[NamedField]).exists(f => s.indexOfField(f.asInstanceOf[NamedField]) == -1)
      case None =>
        // if we don't have a schema all expressions should contain only positional fields
        ! expr.keyList.exists(_.isInstanceOf[NamedField])
    }

  }

//    inputs.map(_.producer.schema).foreach {
//      case Some(s) =>
//        // if we know the schema we check all named fields
//        ! groupExprs.keyList.filter(_.isInstanceOf[NamedField]).exists(f => s.indexOfField(f.asInstanceOf[NamedField]) == -1)
//      case None =>
//        // if we don't have a schema all expressions should contain only positional fields
//        ! groupExprs.keyList.exists(_.isInstanceOf[NamedField])
//    }


  override def lineageString: String = {
    s"""COGROUP%${groupExprs.mkString(",")}%""" + super.lineageString
  }

  override def toString: String =
    s"""COGROUP
       |  out = ${outPipeNames.mkString(",")}
       |  group on  = ${groupExprs.mkString(",")}
       |  in = ${inPipeNames.mkString(",")}
       |  inSchema  = $inputSchema
       |  outSchema = $schema""".stripMargin

}
