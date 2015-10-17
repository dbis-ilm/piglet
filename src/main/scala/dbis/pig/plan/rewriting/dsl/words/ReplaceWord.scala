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
package dbis.pig.plan.rewriting.dsl.words

import dbis.pig.op.PigOperator
import dbis.pig.plan.rewriting.dsl.builders.Builder
import dbis.pig.plan.rewriting.dsl.traits.{BuilderT, EndWordT}

class ReplaceWord[FROM <: PigOperator](val b: BuilderT[FROM, PigOperator]){
  def via(f: (FROM => Option[PigOperator])): RewriteWord[FROM, PigOperator] = {
    b.func = f
    new RewriteWord[FROM, PigOperator](b)
  }
}
