/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbis.piglet.op

import dbis.piglet.schema._
import dbis.piglet.expr.Ref
import dbis.piglet.expr.RefExpr
import dbis.piglet.expr.NamedField


/**
 * Represents the grouping expression for the Grouping operator.
 *
 * @param keyList a list of keys used for grouping
 */
case class GroupingExpression(keyList: List[Ref]) {
  /**
   * Construct the type of the grouping expression
   *
   * @param schema the optional input schema
   * @return the expression type
   */
  def resultType(schema: Option[Schema]): PigType = {
    def typeForRef(r: Ref): PigType = {
       /*
        * We create a temporary expression, because the result type construction is already
        * implemented there.
        */
      val ex = RefExpr(r)
      ex.resultType(schema)
    }

    if (keyList.isEmpty) {
      // GROUP ALL
      Types.CharArrayType
    }
    else if (keyList.size == 1) {
      typeForRef(keyList.head)
    }
    else {
      val resList = keyList.map {
        case r@NamedField(n, _) => (n, typeForRef(r))
        case r => ("", typeForRef(r))
      }.map{ case (n, t) => Field(n, t)}
      TupleType(resList.toArray)
    }
  }
}

/**
 * Grouping represents the GROUP ALL / GROUP BY operator of Pig.
 *
 * @param out the output pipe (relation).
 * @param in the input pipe
 * @param groupExpr the expression (a key or a list of keys) used for grouping
 * @param windowMode true if processed on a window on a data stream
 */
case class Grouping(
    private val out: Pipe, 
    private val in: Pipe, 
    groupExpr: GroupingExpression, 
    var windowMode: Boolean = false
  ) extends PigOperator(out, in) {

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""GROUPBY%$groupExpr%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    // tuple(group: typeOfGroupingExpr, in:bag(inputSchema))
    val inSchema = inputSchema
    val inputType = inSchema match {
      case Some(s) => s.element.valueType
      case None => TupleType(Array(Field("", Types.ByteArrayType)))
    }

    val groupingType = groupExpr.resultType(inputSchema)

    // the group field gets the original grouping expression as lineage, e.g. rel.column
    val groupField = Field("group", groupingType, List(s"$inPipeName.${groupExpr.keyList.mkString}"))

    val aggField = Field(inputs.head.name, BagType(inputType))

    val fields = Array(groupField, aggField)


    schema = Some(Schema(fields))
    schema
  }

  override def checkSchemaConformance: Boolean = {
    inputSchema match {
      case Some(s) =>
        // if we know the schema we check all named fields
        ! groupExpr.keyList.filter(_.isInstanceOf[NamedField]).exists(f => s.indexOfField(f.asInstanceOf[NamedField]) == -1)
      case None =>
        // if we don't have a schema all expressions should contain only positional fields
        ! groupExpr.keyList.exists(_.isInstanceOf[NamedField])
    }
  }


  override def toString: String =
    s"""GROUPING
       |  out = ${outPipeNames.mkString(",")}
       |  in = ${inPipeNames.mkString(",")}
       |  inSchema  = $inputSchema
       |  outSchema = $schema
       |  group on  = $groupExpr""".stripMargin

}


