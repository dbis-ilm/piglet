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

package dbis.piglet.tools

import java.io.{FileOutputStream, InputStream, OutputStream}
import java.net.{ConnectException, URI}
import java.nio.file.{Files, Path}
import java.util.jar.JarFile

import dbis.piglet.tools.logging.PigletLogging
import dbis.setm.SETM.timing

//object ByteUnits extends Enumeration {
//  type ByteUnits = Value
//  val Byte, KB, MB, GB, TB, ZB = Value
//}
//
//import dbis.piglet.tools.ByteUnits._
//case class ByteUnit(value: Long, unit: ByteUnits) {
//  def +(other: ByteUnit): ByteUnit = ???
//}
//
//
//
//object Tools {
//  object Implicits {
////    implicit def Byte(n: Long): ByteUnit = ByteUnit(n, ByteUnits.Byte)
////    implicit def KB(n: Long): ByteUnit = ByteUnit(n, ByteUnits.KB)
////    implicit def MB(n: Long): ByteUnit = ByteUnit(n, ByteUnits.MB)
////    implicit def GB(n: Long): ByteUnit = ByteUnit(n, ByteUnits.GB)
////    implicit def TB(n: Long): ByteUnit = ByteUnit(n, ByteUnits.TB)
////    implicit def ZB(n: Long): ByteUnit = ByteUnit(n, ByteUnits.ZB)
//  }
//}

object FileTools extends PigletLogging {



  def recursiveDelete(dir: Path): Unit = recursiveDelete(dir.toString()) 
  def recursiveDelete(dir: String): Unit = {
    val path = scalax.file.Path.fromString(dir)
    try {
      path.deleteRecursively(continueOnFailure = false)
//      logger.debug(s"removed output directory at $dir")
    }
    catch {
      case e: java.io.IOException => logger.debug(s"Could not remove result directory at ${path}: ${e.getMessage}")
    }
  }
  
  def copyStream(istream: InputStream, ostream: OutputStream): Unit = {
    var bytes = new Array[Byte](1024)
    var len = -1
    while ({ len = istream.read(bytes, 0, 1024); len != -1 })
      ostream.write(bytes, 0, len)
  }


  def extractJarToDir(jarName: String, outDir: Path): Unit = timing("extracting jar") {

    logger.debug(s"extracting jar $jarName to $outDir")

    val jar = new JarFile(jarName)
    val enu = jar.entries
    while (enu.hasMoreElements) {
      val entry = enu.nextElement
      val entryPath = entry.getName

      // we don't need the MANIFEST.MF file
      if (!entryPath.endsWith("MANIFEST.MF")) {

        // println("Extracting to " + outDir + "/" + entryPath)
        if (entry.isDirectory) {
          Files.createDirectories(outDir.resolve(entryPath))
        } else {
          val istream = jar.getInputStream(entry)
          val ostream = new FileOutputStream(outDir.resolve(entryPath).toFile())
          copyStream(istream, ostream)
          ostream.close
          istream.close
        }
      }
    }
  }
  
  /**
   * Send a HTTP GET request to the specified server.
   * The method returns <code>true</code> if the response
   * is a success otherwise <code>false</code>. 
   * 
   * @param url the HTTP URL of the server to check
   * @return Returns <code>true</code> if the server is reachable, otherwise <code>false</code>
   */
  def checkHttpServer(url: URI): Boolean = try {
    scalaj.http.Http(url.toString()).asString.isSuccess
  } catch {
  case e: ConnectException => false
  }
    
}
