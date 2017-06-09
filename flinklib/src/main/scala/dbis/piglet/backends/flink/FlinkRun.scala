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

package dbis.piglet.backends.flink

import java.nio.file.Path

import dbis.piglet.backends.PigletBackend
import dbis.piglet.tools.logging.PigletLogging
import org.apache.flink.client.CliFrontend

class FlinkRun extends PigletBackend with PigletLogging {

  override def execute(master: String, className: String, jarFile: Path, backendArgs: Map[String,String], profiling: Boolean){
    // CliFrontend  needs FLINK_CONF_DIR to be set 
    val args = backendArgs.flatMap { case (k, v) => Array(k) ++ Array(v) }
    if (master.startsWith("local") && !master.startsWith("localhost")){  
      val cli = new CliFrontend
      val ret = cli.parseParameters(Array("run", "-q") ++ args ++ Array("--class", className, jarFile.toString) ++ args)
      logger.info(s"completed with status code $ret")
    }
    else {
      val cli = new CliFrontend
      val ret = cli.parseParameters(Array("run", "-q", "--jobmanager", master) ++ args ++ Array("--class", className, jarFile.toString))
      logger.info(s"completed with status code $ret")
    }
  }

  override def executeRaw(file: Path, master: String, backendArgs: Map[String,String]) = ???
}
