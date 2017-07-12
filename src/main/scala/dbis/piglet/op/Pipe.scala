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
package dbis.piglet.op

/**
 * An exception indicating that the name of the pipe isn't a valid identifier (e.g. contains still a leading '$').
 *
 * @param msg a message describing the exception.
 */
case class InvalidPipeNameException(private val msg: String) extends Exception("invalid pipe name: " + msg)


/**
 * A pipe connects some Pig operator and associates a name to this channel.
 *
 * @param name the name of the pipe
 * @param producer the operator producing the data
 * @param consumer the list of operators reading this data
 */
class Pipe (var name: String, var producer: PigOperator = null, var consumer: List[PigOperator] = List()) extends Serializable {
  override def toString = s"Pipe($name)"

  def canEqual(a: Any) = a.isInstanceOf[Pipe]

  override def equals(that: Any): Boolean =
    that match {
      case that: Pipe => that.canEqual(this) && this.name == that.name
      case _ => false
    }

  override def hashCode = name.hashCode

  def inputSchema = if (producer != null) producer.schema else None

  def removeConsumer(op:PigOperator): Unit = this.consumer = this.consumer.filterNot(_ == op)

  def addConsumer(op: PigOperator): Unit = this.consumer = this.consumer.filterNot(_ == op) :+ op
}

object Pipe {
  def apply(n: String, producer: PigOperator = null, consumers: List[PigOperator] = List()): Pipe =
    new Pipe(n, producer, consumers)

  def unapply(p: Pipe): Option[(String, PigOperator, List[PigOperator])] = Some((p.name, p.producer, p.consumer))

  def dummy = Pipe("dummy")
}