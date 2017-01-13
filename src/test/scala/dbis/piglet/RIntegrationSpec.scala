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
package dbis.piglet

import dbis.piglet.parser.PigParser.parseScript
import dbis.piglet._
import dbis.piglet.op._
import dbis.piglet.schema._
import org.scalatest.{FlatSpec, Matchers}
import org.ddahl.jvmr.RInScala
import scala.io.Source
import org.scalatest.Tag

object RTest extends Tag("R")

class RIntegrationSpec extends FlatSpec with Matchers {
  def checkForWorkingR(): Boolean = {
    try {
      val R = RInScala()
      true
    } catch {
      case e: Exception => false
    }
  }

//  "The R integration" 
  it should "allow to invoke a R script" taggedAs(RTest) in {
    if (checkForWorkingR) {
      val R = RInScala()
      R.x = Array(10.0, 20.0, 30.0)
      R.y = Array(5.0, 6.0, 7.0)
      R.eval("res <- x + y")
      val res = R.toVector[Double]("res")
      res should be(Array(15.0, 26.0, 37.0))
    }
    else
      assume(false, "R not enabled, no test performed")

  }

  it should "run DBSCAN in R" taggedAs(RTest) in {
    if (checkForWorkingR) {
    /**
     * Prepare the data in R as follows:
     * > data(ruspini, package="cluster")
     * > ruspini ruspini[sample(1:nrow(ruspini)),]
     * > ruspini = scale(ruspini)
     * > finalData = ruspini[order(as.numeric(rownames(ruspini))),,drop=FALSE]
     * > write.table(finalData, file = "data.csv",row.names=FALSE,col.names=FALSE,sep=",")
     */
    val source = Source.fromFile("./src/test/resources/cluster-data.csv")
    val input = source.getLines().map(line => line.split(","))
    val matrix = input.map(v => v.map(s => s.toDouble))

    val script =
      """
        |library(fpc);
        |db = dbscan(inp, eps=.3, MinPts=5);
        |cluster = cbind(inp, data.frame(db$cluster + 1L))
        |res = data.matrix(cluster)
        |""".stripMargin
    val R = RInScala()
    R.inp = matrix.toArray
    R.eval(script)
    val res = R.toMatrix[Double]("res")
    res.length should be (75)
    println(res)
    }
    else
      assume(false, "R not enabled, no test performed")
  }

//  "The parser"
  it should "accept the SCRIPT statement" taggedAs(RTest) in  {
    parseScript("""a = RSCRIPT b USING 'library(fpc); res <- dbscan($_, eps=0.42, MinPts=5)';""")
  }

}
