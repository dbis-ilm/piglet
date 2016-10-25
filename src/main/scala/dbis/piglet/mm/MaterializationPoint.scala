package dbis.piglet.mm

/**
 * A MaterializationPoint object represents information about a possible materialization of the result
 * of a dataflow operator. It is identified by a hash of the lineage string of the operator and collects
 * profile information.
 *
 * @param hash the MD5 hash of the lineage string of the operator
 * @param parentHash the MD5 hash of the parent's lineage string
 * @param count the number of (potential) usages
 * @param benefit the cumulative benefit of this materialization point compared to the root operator
 * @param procTime the (normalized processing time of the operator corresponding to this materialization point
 * @param loadTime the (normalized) time to load the data corresponding to this materialization point
 * @param saveTime the (normalized) time to save the output of the operator corresponding to this materialization
 * @param isMaterialized true of data corresponding to this point is materialized in a file
 */
case class MaterializationPoint(hash: String,
                                parentHash: Option[String],
                                var procTime: Long, var loadTime: Long, var saveTime: Long,
                                var count: Int = 0, var benefit: Long = 0,
                                var isMaterialized: Boolean = false)
