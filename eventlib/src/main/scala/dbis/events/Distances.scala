package dbis.events

/**
 * Created by kai on 01.06.15.
 */
object Distances {
  def spatialDistance(longitude: Double, latitude: Double, refLongitude: Double, refLatitude: Double): Double = 1.0
  def temporalDistance(startTime: Long, endTime: Long, refTime: Long): Long = 1
}
