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

package dbis.pig.op

import dbis.pig.plan.rewriting.internals.RDF
import dbis.pig.schema._
import dbis.pig.expr.Ref


case class TriplePattern(subj: Ref, pred: Ref, obj: Ref)
/**
 *
 * @param out
 * @param in
 * @param patterns
 */
case class BGPFilter(out: Pipe, in: Pipe, patterns: List[TriplePattern]) extends PigOperator {
  _outputs = List(out)
  _inputs = List(in)

  override def lineageString: String = s"""BGPFilter%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    if (inputSchema.isEmpty) {
      return false
    }

//    if (inputSchema == RDFLoad.plainSchema) {
    if(inputSchema.get.fields.sameElements(RDFLoad.plainSchema.get.fields)) {
      return true
    }
    if (RDFLoad.groupedSchemas.values.toList contains inputSchema.get) {
      return true
    }

    false
  }

  override def constructSchema: Option[Schema] = {
    if (patterns.length < 2) {
      schema = inputSchema
    } else {
      val variables = RDF.getAllVariables(patterns)
      var fields = List[Field]()
      variables foreach { v =>
        fields = fields :+ Field(v.name, Types.CharArrayType)
      }
      fields = fields.sortWith((f1, f2) => f2.name.compareTo(f2.name) >= 0)
      schema = Some(Schema(BagType(TupleType(fields.toArray))))
    }
    schema
  }
}

