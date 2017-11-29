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
package dbis.piglet.plan.rewriting.internals

import java.net.URI

import dbis.piglet.mm.MaterializationManager
import dbis.piglet.op.Materialize
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.{BreadthFirstBottomUpWalker, CliParams}
import dbis.piglet.tools.logging.PigletLogging
import dbis.setm.SETM.timing

import scala.collection.mutable.ListBuffer

/** Provides methods to deal with Materialization in a [[dbis.piglet.plan.DataflowPlan]].
  *
  */
trait MaterializationSupport extends PigletLogging {
  def processMaterializations(plan: DataflowPlan, mm: MaterializationManager): DataflowPlan = timing("process materialization") {
    require(plan != null, "Plan must not be null")
    require(mm != null, "Materialization Manager must not be null")

    val materializes = ListBuffer.empty[Materialize]

    BreadthFirstBottomUpWalker.walk(plan) {
      case o: Materialize => materializes += o
      case _ =>
    }

    logger.debug(s"found ${materializes.size} materialize operators")

    var newPlan = plan

    /* we should check here if the op is still connected to a sink
     * the ops will all still be in the plan, but they might be disconnected
     * if a load was inserted before
     */
    for (materialize <- materializes if newPlan.containsOperator(materialize)) {

      val data = mm.getDataFor(materialize.lineageSignature)

      /*
       * The materialization manager has data for the current materialization
       * operator. So create a new Load operator for the materialized result
       * and add it to the plan by replacing the input of the Materialize-Op
       * with the loader.
       */
      if(data.isDefined) {
        logger.info(s"found materialized data for materialize operator $materialize")

        val path: URI = data.get

        newPlan = MaterializationManager.replaceWithLoad(materialize, path, newPlan)

      } else {
        /* there is a MATERIALIZE operator, for which no results could be found
         * --> store them by adding a STORE operator to the MATERIALIZE operator's input op
         * then, remove the materialize op
         */
        logger.info(s"did not find materialized data for materialize operator $materialize")

        val sig = materialize.lineageSignature

        val file = mm.generatePath(sig)

      if(!CliParams.values.compileOnly)
          mm.saveMapping(sig, file)

        newPlan = MaterializationManager.replaceWithStore(materialize, file, newPlan)
      }
    }

    newPlan
  }


}
