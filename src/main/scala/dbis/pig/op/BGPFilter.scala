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

import dbis.pig.schema.Schema


case class TriplePattern(subj: Ref, pred: Ref, obj: Ref)
/**
 *
 * @param initialOutPipeName the name of the initial output pipe (relation) which is needed to construct the plan, but
 *                           can be changed later.
 * @param initialInPipeName
 * @param opName
 * @param params
 * @param loadSchema
 */
case class BGPFilter(out: Pipe, in: Pipe, patterns: List[TriplePattern]) extends PigOperator {
  _outputs = List(out)
  _inputs = List(in)

  override def lineageString: String = s"""BGPFilter%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    // TODO
    true
  }

  schema = inputSchema

  override def constructSchema: Option[Schema] = {
    // TODO
    schema
  }
}

