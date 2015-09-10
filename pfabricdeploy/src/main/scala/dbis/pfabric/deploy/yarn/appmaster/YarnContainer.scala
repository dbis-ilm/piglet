package dbis.pfabric.deploy.yarn.appmaster

import org.apache.hadoop.yarn.api.records.Container
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.format._
import org.joda.time.Period

object YarnContainerUtils {
  val dateFormater = ISODateTimeFormat.dateTime
  val periodFormater = ISOPeriodFormat.standard
}

/**
 * YARN container information plus start time and up time
 */
class YarnContainer(container: Container) {
  val id = container.getId()
  val nodeId = container.getNodeId();
  val nodeHttpAddress = container.getNodeHttpAddress();
  val resource = container.getResource();
  val priority = container.getPriority();
  val containerToken = container.getContainerToken();
  val startTime = System.currentTimeMillis()
  def startTimeStr(dtFormatter: Option[DateTimeFormatter] = None) =
    dtFormatter.getOrElse(YarnContainerUtils.dateFormater).print(startTime)
  def upTime = System.currentTimeMillis()
  def upTimeStr(periodFormatter: Option[PeriodFormatter] = None) =
  periodFormatter.getOrElse(YarnContainerUtils.periodFormater).print(new Period(startTime, upTime))
}