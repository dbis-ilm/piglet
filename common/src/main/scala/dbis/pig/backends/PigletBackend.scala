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

package dbis.pig.backends

import java.nio.file.Path

/**
 * Defines the interface to the backend execution.
 */
trait PigletBackend {
  
  /**
   * Execute the job using this backend
   * 
   * @param master The master information (execution mode)
   * @param className The className of the main class in the job
   * @param jarFile The path to the jar file representing the job
   * @param backendArgs Argument list passed to the job executor
   */
  def execute(master: String, className: String, jarFile: Path, backendArgs: Map[String,String]): Unit

  /**
   * Execute the raw Pig script. Use this function if your engine does not rely on
   * code generation by Piglet but rather needs to process the Pig script directly
   * by itself 
   * 
   * @param master The master information (execution mode)
   * @param program The path to the script file
   * @param backendArgs Argument list passed to the job executor
   */
  def executeRaw(program: Path, master: String, backendArgs: Map[String,String])
}
