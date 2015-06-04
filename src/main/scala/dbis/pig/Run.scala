package dbis.pig

/**
  * Created by philipp on 04.06.15.
  */

/**
  * Defines the interface to the backend execution.
  */
trait Run {
  def execute(master: String, className: String, jarFile: String): Unit
}
