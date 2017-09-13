package dbis.piglet.op

/**
 * This represent a MATERIALIZE operator in Pig
 * 
 */
case class Materialize(private val in: Pipe) extends PigOperator(List(), List(in)) {
  
  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""MATERIALIZE%""" + super.lineageString
  }

  override def toString =
    s"""MATERIALIZE
       |  in = $inPipeName
     """.stripMargin
}