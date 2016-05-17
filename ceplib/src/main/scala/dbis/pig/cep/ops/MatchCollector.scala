package dbis.pig.cep.ops
import dbis.pig.cep.nfa.NFAStructure
import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import dbis.pig.backends.{SchemaClass => Event}

class MatchCollector[ T <: Event: ClassTag] extends Serializable {
  var macthSequences: ListBuffer[NFAStructure[T]] = new ListBuffer()
  def +(that: NFAStructure[T]): Unit = macthSequences += that
  def size: Int = macthSequences.size
  def convertEventsToArray(): ArrayBuffer[T] = {
    var events: ArrayBuffer[T] = new ArrayBuffer()
    macthSequences.foreach (seq =>  events ++= seq.events)
    events
  }
  def convertEventsToBoolean(): ArrayBuffer[Boolean] = {
    ArrayBuffer(macthSequences.size > 0)
  }
}