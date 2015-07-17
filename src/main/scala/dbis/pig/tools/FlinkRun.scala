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

import org.apache.flink.client.CliFrontend
import org.apache.flink.client.RemoteExecutor
import org.apache.flink.client.program.Client
import org.apache.flink.client.program.PackagedProgram
import org.apache.flink.client.program.ProgramInvocationException
import org.apache.flink.configuration.Configuration

import java.io.File
import java.net.InetSocketAddress
import java.net.URI
import java.net.URISyntaxException

import org.apache.log4j.Logger
import org.apache.log4j.Level


class FlinkRun extends Run{
  override def execute(master: String, className: String, jarFile: String){
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("Remoting").setLevel(Level.WARN)

    if (master.startsWith("local") && !master.startsWith("localhost")){
 //     val cli = new CliFrontend
 //     val ret = cli.parseParameters(Array("run", "--class", className, jarFile))
      submitJar("localhost:6123", jarFile, className)
    }
    else {
      val cli = new CliFrontend
      val ret = cli.parseParameters(Array("run", "--jobmanager", master, "--class", className, jarFile))
    }

  }

  def submitJar(master: String, path: String, className: String, args: String*) = { 
    val file = new File(path)
    val parallelism = 1 
    val wait = true
    try { 
      val program = new PackagedProgram(file, className, args:_*)
      val jobManagerAddress = getInetFromHostport(master)
      val client = new Client(jobManagerAddress, new Configuration(), program.getUserCodeClassLoader(), 1)  
      println("Executing " + path); 
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
