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
package dbis.piglet.plan.rewriting

import dbis.piglet.plan.rewriting.rulesets.{GeneralRuleset, RDFRuleset, SparkRuleset}
import dbis.piglet.tools.CliParams
import dbis.piglet.tools.logging.PigletLogging



/** This object contains all the rewriting rules that are currently implemented
  *
  */
//noinspection ScalaDocMissingParameterDescription
object Rules extends PigletLogging {
  val rulesets = (
    if(CliParams.values.optimization) List(GeneralRuleset) else {
    logger.debug("disabling general optimization rules!")
    List.empty
  }) ++ List(RDFRuleset)

  def registerAllRules() = {
    // IMPORTANT: If you change one of the rule registration calls in here, please also change the call in the
    // corresponding test methods!
    rulesets foreach { _.registerRules() }
  }

  def registerBackendRules(backend: String) = backend match {
    case "spark" if CliParams.values.optimization => SparkRuleset.registerRules()
    case "spark" => logger.debug("will no optimize with Spark specific rules!")
    case _ =>
  }
}
