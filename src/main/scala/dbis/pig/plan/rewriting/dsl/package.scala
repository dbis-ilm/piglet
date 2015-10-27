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
package dbis.pig.plan.rewriting

/** This package provides a DSL for adding rewriting rules.
  *
  * The entry point to the DSL is the [[dbis.pig.plan.rewriting.dsl.RewriterDSL]] trait which is mixed into
  * [[dbis.pig.plan.rewriting.Rewriter]].
  *
  * ==General methods==
  *
  * Several general methods are provided that can be used for any rewriting operation:
  *
  * - [[dbis.pig.plan.rewriting.dsl.RewriterDSL.applyPattern]]
  *
  * - [[dbis.pig.plan.rewriting.dsl.RewriterDSL.applyRule]]
  *
  * To help with conditional rewriting,
  *
  * - [[dbis.pig.plan.rewriting.dsl.RewriterDSL.when]]
  *
  * - [[dbis.pig.plan.rewriting.dsl.RewriterDSL.whenMatches]]
  *
  * - [[dbis.pig.plan.rewriting.dsl.RewriterDSL.unless]]
  *
  * - [[dbis.pig.plan.rewriting.dsl.RewriterDSL.unlessMatches]]
  *
  * can be used to add conditions to a rewriting operation.
  *
  * ===Return value===
  *
  * A rewriting operation can return objects of several types. For some of them, additional behaviour is applied
  * automatically:
  *
  * - If the return value is of type ``Tuple2[PigOperator, PigOperator]``, operation will be treated as a replacement
  * with multiple operators. The tuples first element is treated as the new flows first operator - it will get the
  * inputs of the rewriting operations input operator. The tuples second element is treated as the new flows last
  * operator - it will get the outputs of the rewriting operations input operator.
  *
  * - If the return value is of type [[dbis.pig.op.PigOperator]], the operation will be treated as a replacement with
  * a single operator which will get the inputs and outputs of the rewriting operations input operator.
  *
  * - Values of all other types will not be treated specially. In particular, nothing will be done to a
  * `Seq[PigOperator]` or its elements.
  *
  * ==Specific methods==
  *
  * For common rewriting operations, the following methods are provided:
  *
  * - [[dbis.pig.plan.rewriting.dsl.RewriterDSL.toMerge]] for adding an operation that merges two operators
  *
  * - [[dbis.pig.plan.rewriting.dsl.RewriterDSL.toReplace]] for adding a rule that replaces an operator with a
  * different one.
  *
  * ==Related objects==
  *
  * To help in creating rewriting rules, two objects provide additional helper methods:
  *
  * - [[dbis.pig.plan.rewriting.Extractors]] includes several extractor objects. These can be used on the left-hand
  * side of patterns.
  *
  * - [[dbis.pig.plan.rewriting.Functions]] includes several functions for manipulating the data flow between
  * [[dbis.pig.op.PigOperator]] objects. These can be used on the left-hand side of patterns and in regular functions.
  *
  */
package object dsl {

}
