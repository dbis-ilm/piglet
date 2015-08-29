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

package dbis.flink

import scala.sys.process._
import java.security._
import org.apache.flink.client.CliFrontend
import dbis.pig.backends.PigletBackend
import com.typesafe.config.ConfigFactory
import dbis.pig.backends.BackendConf
import java.nio.file.Path
import java.io.File
import org.apache.flink.client.program.PackagedProgram
import org.apache.flink.configuration.Configuration
import org.apache.flink.client.program.Client
import org.apache.flink.client.program.ProgramInvocationException
import java.net.URI
import java.net.URISyntaxException
import java.net.InetSocketAddress

import com.typesafe.scalalogging.LazyLogging

class FlinkRun extends PigletBackend with LazyLogging {
  
  override def execute(master: String, className: String, jarFile: Path, numExecutors: Int){
    if (master.startsWith("local") && !master.startsWith("localhost")){
//      val cli = new CliFrontend
//      val ret = cli.parseParameters(Array("run", "--class", className, jarFile.toString()))
      submitJar("localhost:6123", numExecutors, jarFile, className)
    }
    else {
//      val cli = new CliFrontend
//      val ret = cli.parseParameters(Array("run", "--jobmanager", master, "--class", className, jarFile.toString()))
      submitJar(master, numExecutors, jarFile, className)
    }
  }

  override def executeRaw(file: Path, master: String, numExecutors: Int) = ???
  
  def submitJar(master: String, numExecutors: Int, path: Path, className: String, args: String*) = { 
    val file = path.toFile().getAbsoluteFile()
    val parallelism = if(numExecutors <= 0) 1 else numExecutors 
    val wait = true
    try { 
      logger.debug(s"submitting $file")
      val program = new PackagedProgram(file, className, args:_*)
      val jobManagerAddress = getInetFromHostport(master)
      logger.debug(s"using job manager at $jobManagerAddress  for name $master")
      val client = new Client(jobManagerAddress, new Configuration(), program.getUserCodeClassLoader(), 1)
      logger.debug(s"created job client: $client")
      println(s"Executing ${path.toString}"); 
      client.run(program, parallelism, wait); 
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
