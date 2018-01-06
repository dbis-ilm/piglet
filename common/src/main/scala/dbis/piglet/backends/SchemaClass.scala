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

/**
  * The trait for all case classes implementing record types in Piglet.
  */
trait SchemaClass extends Product {
  /**
    * Produces a string representation of the object using the given delimiter.
    *
    * @param delim the delimiter string
    * @return a string representation
    */
  def mkString(delim: String = ","): String

  /**
    * Overrides the default toString method.
    *
    * @return a string representation
    */
  override def toString = s"(${mkString()})"

  /**
    * Returns the timestamp of the tuple - either application or system time.
    * The default implementation returns always 0 and has to be overwritten
    * in subclasses.
    *
    * @return the timestamp of the tuple
    */
  def timestamp(): Long = 0
}

/**
  * A record class for an array of string values.
  *
  * @param fields the array of values
  */
case class Record(fields: Array[String]) extends java.io.Serializable with SchemaClass {
  override def mkString(delim: String) = fields.mkString(delim)

//  override lazy val getNumBytes: Int = fields.map(_.getBytes.length).sum

  /**
    * Returns the value for field at position idx
    *
    * @param idx the field position
    * @return the value of the field
    */
  def get(idx: Int) = fields(idx)
}