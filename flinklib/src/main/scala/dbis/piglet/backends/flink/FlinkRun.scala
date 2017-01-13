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

import scala.sys.process._
import java.security._
import org.apache.flink.client.CliFrontend
import dbis.piglet.backends.PigletBackend
import com.typesafe.config.ConfigFactory
import dbis.piglet.backends.BackendConf
import java.nio.file.Path
import java.io.File
import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.GlobalConfiguration
import java.net.URI
import java.net.URISyntaxException
import java.net.InetSocketAddress

import dbis.piglet.tools.logging.PigletLogging

class FlinkRun extends PigletBackend with PigletLogging {

  override def execute(master: String, className: String, jarFile: Path, backendArgs: Map[String,String]){
    // CliFrontend  needs FLINK_CONF_DIR to be set 
    val args = backendArgs.map {case (k, v) => (Array(k) ++ Array(v))}.flatten
    if (master.startsWith("local") && !master.startsWith("localhost")){  
      val cli = new CliFrontend
      val ret = cli.parseParameters(Array("run", "-q") ++ args ++ Array("--class", className, jarFile.toString()) ++ args)
    }
    else {
      val cli = new CliFrontend
      val ret = cli.parseParameters(Array("run", "-q", "--jobmanager", master) ++ args ++ Array("--class", className, jarFile.toString()))
    }
  }

  override def executeRaw(file: Path, master: String, backendArgs: Map[String,String]) = ???
}
