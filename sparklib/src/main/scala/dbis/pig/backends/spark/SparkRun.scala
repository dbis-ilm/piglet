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

package dbis.pig.backends.spark

import org.apache.spark.deploy.SparkSubmit
import org.apache.log4j.Logger
import org.apache.log4j.Level
import dbis.pig.backends.PigletBackend
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import scala.collection.JavaConversions._
import dbis.pig.backends.BackendConf
import java.nio.file.Path

class SparkRun extends PigletBackend with BackendConf {

  // loads the default configuration file in resources/sparkbackend.conf
  private val appconf = ConfigFactory.load() 
  
  override def execute(master: String, className: String, jarFile: Path) {
    SparkSubmit.main(Array("--master", master, "--class", className, jarFile.toAbsolutePath().toString()))
  }
  
  override def executeRaw(file: Path, master: String) = ???
  
  /**
   * Get the name of this backend
   * 
   * @return Returns the name of this backend
   */
  override def name: String = appconf.getString("backends.spark.name")
  
  /**
   * Get the path to the runner class that implements the PigletBackend interface
   */
  override def runnerClass: PigletBackend = {
    this
  } 
  
  override def templateFile = appconf.getString("backends.spark.template")
  
  override def raw = false
}
