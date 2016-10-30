package dbis.piglet.cep.ops
import scala.reflect.ClassTag
import dbis.piglet.backends.{SchemaClass => Event}
package object Outputter {
  def convertEventsToArray[T <: Event: ClassTag](collector: MatchCollector[T]): Any = {
    collector.convertEventsToArray()
  }
  def convertEventsToBoolean[T <: Event: ClassTag](collector: MatchCollector[T]): Any = {
    collector.convertEventsToBoolean()
  }
}