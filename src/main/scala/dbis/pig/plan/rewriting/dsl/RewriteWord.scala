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
package dbis.pig.plan.rewriting.dsl

import scala.reflect.ClassTag

class RewriteWord[T : ClassTag](val cls: Class[_]) extends Word {
  def via(f: T => Option[Any]): Unit = {
    ViaWord(f)
  }

  def unless(check: (T => Boolean)): WhenWord[T] = {
    val newCheck = {term: T =>
      (!check(term))
    }
    when(newCheck)
  }

  def when(check: (T => Boolean)): WhenWord[T] = {
    new WhenWord(check)
  }
}