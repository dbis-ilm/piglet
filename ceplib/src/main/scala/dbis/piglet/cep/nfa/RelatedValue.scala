package dbis.piglet.cep.nfa
import scala.reflect.ClassTag
import dbis.piglet.backends.{SchemaClass => Event}

case class NotInitializedException(private val msg: String) extends Exception(msg)

trait RelatedValue[T]{
    def updateValue(event: T): Unit
    def initValue(event: T): Unit
    def getValue(): Double
}

abstract class PreviousRelatedValue[T <: Event: ClassTag] extends RelatedValue[T]{
  var value: Option[Double] = None
  override def initValue(event: T): Unit = updateValue(event)
  override def updateValue(event: T): Unit
  override def getValue(): Double = {
    value match {
      case Some(v) => v
      case None => throw NotInitializedException("Related value is not initialized")
    }
  }
}