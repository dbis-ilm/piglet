package dbis.piglet.backends.spark

import dbis.piglet.backends.SchemaClass

case class Person(name: String, age: Int) extends java.io.Serializable with SchemaClass {
  override def mkString(delim: String) = s"$name$delim$age"

  override lazy val getNumBytes: Int = name.getBytes.length + 4
}
