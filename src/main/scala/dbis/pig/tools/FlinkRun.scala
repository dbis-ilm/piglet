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

package dbis.pig.tools

import scala.sys.process._

class FlinkRun extends Run{
  override def execute(master: String, className: String, jarFile: String){
    val flinkJar = sys.env.get("FLINK_JAR") match {
      case Some(n) => n
      case None => throw new Exception(s"Please set FLINK_JAR to your flink-dist jar file")
    }
    val run = s"java -Dscala.usejavacp=true -cp ${flinkJar}:${jarFile} ${className}"
    println(run)
    run !
  }
}
