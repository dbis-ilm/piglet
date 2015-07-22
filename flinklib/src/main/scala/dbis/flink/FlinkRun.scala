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
import java.security._
import org.apache.flink.client.CliFrontend
import dbis.pig.backends.PigletBackend
import com.typesafe.config.ConfigFactory


class FlinkRun extends PigletBackend {
  
  // loads the default configuration file in resources/sparkbackend.conf
  private val appconf = ConfigFactory.load("flinkbackend.conf")
  
  /**
   * Get the name of this backend
   * 
   * @return Returns the name of this backend
   */
  override def name: String = appconf.getString("backend.name")
  
  /**
   * Get the path to the runner class that implements the PigletBackend interface
   */
  override def runnerClass: PigletBackend = {
    this
  } 
  
  override def jobJar: String = appconf.getString("backend.jar")
  
  override def templateFile: String = appconf.getString("backend.template")
  
  override def execute(master: String, className: String, jarFile: String){
    if (master.startsWith("local") && !master.startsWith("localhost")){
      //      val zmqJar = "/home/blaze/.ivy2/cache/org.zeromq/zeromq-scala-binding_2.11.0-M3/jars/zeromq-scala-binding_2.11.0-M3-0.0.7.jar"
      //      val pigJar = "/home/blaze/Masterthesis/projects/pigspark/target/scala-2.11/PigCompiler.jar"
      /*      
       val flinkJar = sys.env.get("FLINK_JAR") match {
         case Some(n) => n
         case None => throw new Exception(s"Please set FLINK_JAR to your flink-dist jar file")
       }
       val run = s"java -Dscala.usejavacp=true -cp ${flinkJar}:${pigJar}:${jarFile} ${className}"
       println(run)
       run !
       */
      val cli = new CliFrontend
      val ret = cli.parseParameters(Array("run", "--class", className, jarFile))
    }
    else {
      val cli = new CliFrontend
      val ret = cli.parseParameters(Array("run", "--jobmanager", master, "--class", className, jarFile))
    }

  }
}
