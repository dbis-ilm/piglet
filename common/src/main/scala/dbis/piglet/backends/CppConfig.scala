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

package dbis.piglet.backends
import scala.collection.immutable.List


/**
 * Defines the interface to the c++ compiler
 */
trait CppConfig {

  /**
   * Get a C++ compiler, this can be done g++, clang++, ....
   */
  def getCompiler: String

  /**
   * Get the libraries which are used during compiling. The compiler has to link to these
   * libraries otherwise, linking error will be shown
   */
  def getLibraries: List[String]

  /**
   * Get options for compiling the code accordingly such as the optimization level, enabling some
   * macros, etc.
   */
  def getOptions: List[String]
  /**
   * Get directories for libraries which are essential during the linking
   */
  def getLibDirs: List[String]
  /**
   *  Get include directories for finding the header files.
   */
  def getIncludeDirs: List[String]
}
