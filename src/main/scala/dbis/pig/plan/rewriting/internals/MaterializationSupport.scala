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
package dbis.pig.plan.rewriting.internals
import java.net.URI

import dbis.pig.tools.logging.PigletLogging
import dbis.pig.op.{Load, Materialize, Store}
import dbis.pig.plan.{DataflowPlan, MaterializationManager}
import dbis.pig.tools.BreadthFirstBottomUpWalker

import scala.collection.mutable.ListBuffer

/** Provides methods to deal with Materialization in a [[dbis.pig.plan.DataflowPlan]].
  *
  */
trait MaterializationSupport extends PigletLogging {
  def processMaterializations(plan: DataflowPlan, mm: MaterializationManager): DataflowPlan = {
    require(plan != null, "Plan must not be null")
    require(mm != null, "Materialization Manager must not be null")

    val walker = new BreadthFirstBottomUpWalker

    val materializes = ListBuffer.empty[Materialize]

    walker.walk(plan) { op =>
      op match {
        case o: Materialize => materializes += o
        case _ =>
      }
    }

    logger.debug(s"found ${materializes.size} materialize operators")

    var newPlan = plan

    /* we should check here if the op is still connected to a sink
     * the ops will all still be in the plan, but they might be disconnected
     * if a load was inserted before
     */
    for (materialize <- materializes if plan.containsOperator(materialize)) {

      val data = mm.getDataFor(materialize.lineageSignature)

      /*
       * The materialization manager has data for the current materialization
       * operator. So create a new Load operator for the materialized result
       * and add it to the plan by replacing the input of the Materialize-Op
       * with the loader.
       */
      if(data.isDefined) {
        logger.debug(s"found materialized data for materialize operator $materialize")

        val loader = Load(materialize.inputs.head, new URI(data.get), materialize.constructSchema, Some("BinStorage"))
        val matInput = materialize.inputs.head.producer

        for (inPipe <- matInput.inputs) {
          plan.disconnect(inPipe.producer, matInput)
        }

        newPlan = plan.replace(matInput, loader)

        logger.info(s"replaced materialize op with loader $loader")

        /* TODO: do we need to remove all other nodes that get disconnected now by hand
         * or do they get removed during code generation (because there is no sink?)
         */
        newPlan = newPlan.remove(materialize)

      } else {
        /* there is a MATERIALIZE operator, for which no results could be found
         * --> store them by adding a STORE operator to the MATERIALIZE operator's input op
         * then, remove the materialize op
         */
        logger.debug(s"did not find materialized data for materialize operator $materialize")

        val file = mm.saveMapping(materialize.lineageSignature)
        val storer = new Store(materialize.inputs.head, file, Some("BinStorage"))

        newPlan = plan.insertAfter(materialize.inputs.head.producer, storer)
        newPlan = newPlan.remove(materialize)

        logger.info(s"inserted new store operator $storer")
      }
    }

    newPlan
  }

}
