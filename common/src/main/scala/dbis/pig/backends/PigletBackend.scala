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

/**
 * Defines the interface to the backend execution.
 */
trait PigletBackend {
  
  /**
   * Get the name of this backend
   * 
   * @return Returns the name of this backend
   */
  def name: String
  
  /**
   * Get the path to the runner class that implements the PigletBackend interface. 
   * This class can be used to submit jobs
   * 
   * @return Returns the full qualified name of the runner class
   */
  def runnerClass: PigletBackend
  
  /**
   * Get the path to the jar file containing the job's code
   * 
   * @return Returns the path the the job's jar file
   */
  def jobJar: String
  
  /**
   * Get the full path to the template file to use for the backend
   * 
   * @return the name of the template file
   */
  def templateFile: String
  
  
  /**
   * Execute the job using this backend
   * 
   * @param master The master information (execution mode)
   * @param className The className of the main class in the job
   * @param jarFile The path to the jar file representing the job
   */
  def execute(master: String, className: String, jarFile: String): Unit
}
