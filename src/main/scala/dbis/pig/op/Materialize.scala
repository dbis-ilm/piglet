package dbis.pig.op

/**
 * This represent a MATERIALIZE operator in Pig
 * 
 */
case class Materialize(initialInPipeName: String) extends PigOperator("", initialInPipeName) {
  
  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""MATERIALIZE%""" + super.lineageString
  }
  
}