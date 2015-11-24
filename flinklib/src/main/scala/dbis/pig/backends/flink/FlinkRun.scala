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

package dbis.pig.backends.flink

import scala.sys.process._
import java.security._
import org.apache.flink.client.CliFrontend
import dbis.pig.backends.PigletBackend
import com.typesafe.config.ConfigFactory
import dbis.pig.backends.BackendConf
import java.nio.file.Path
import java.io.File
import org.apache.flink.client.program.Client
import org.apache.flink.client.program.PackagedProgram
import org.apache.flink.client.program.ProgramInvocationException
import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.GlobalConfiguration
import java.net.URI
import java.net.URISyntaxException
import java.net.InetSocketAddress

import dbis.pig.tools.logging.PigletLogging

class FlinkRun extends PigletBackend with PigletLogging {

  override def execute(master: String, className: String, jarFile: Path, backendArgs: Map[String,String]){
    if (master.startsWith("local") && !master.startsWith("localhost")){  
      //val cli = new CliFrontend
      //val ret = cli.parseParameters(Array("run", "--class", className, jarFile.toString()))
      submitJar("localhost:6123", jarFile, backendArgs, className)
    }
    else {
      //val cli = new CliFrontend
      //val ret = cli.parseParameters(Array("run", "--jobmanager", master, "--class", className, jarFile.toString()))
      submitJar(master, jarFile, backendArgs, className)
    }
  }

  override def executeRaw(file: Path, master: String, backendArgs: Map[String,String]) = ???
  
  def submitJar(master: String, path: Path, backendArgs: Map[String,String], className: String, args: String*) = {

    val file = path.toFile().getAbsoluteFile()
    
    val parallelism = backendArgs.getOrElse("parallelism", "1").toInt
    
    try { 

      logger.debug(s"submitting $file")

      val program = new PackagedProgram(file, className, args:_*)
      
      val configuration = new Configuration()
      val jobManagerAddress = getInetFromHostport(master)
      logger.debug(s"using job manager at $jobManagerAddress for name $master")
      configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerAddress.getHostName())
      configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerAddress.getPort())

      val client = new Client(configuration,  1)
      logger.debug(s"created job client: $client")
      println(s"Executing ${path.toString}") 
      client.runBlocking(program, parallelism)

    } catch {
      case e: ProgramInvocationException => e.printStackTrace()
    }   
  }

  def getInetFromHostport(hostport: String) = {
    // from http://stackoverflow.com/questions/2345063/java-common-way-to-validate-and-convert-       hostport-to-inetsocketaddress
    var uri = null.asInstanceOf[URI]
    try {
      uri = new URI("my://" + hostport)
    } catch {
      case e: URISyntaxException =>
        throw new RuntimeException("Could not identify hostname and port in '" + hostport + "'.", e)
    }       
    val host = uri.getHost()
    val port = uri.getPort()
    if (host == null || port == -1) {
      throw new RuntimeException("Could not identify hostname and port in '" + hostport + "'.")
    }
    new InetSocketAddress(host, port)

  }
}
