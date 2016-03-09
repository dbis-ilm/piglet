package dbis.setm

import etm.core.configuration.BasicEtmConfigurator
import etm.core.configuration.EtmManager
import etm.core.renderer.{MeasurementRenderer, SimpleTextRenderer}

/**
 * SETM is a simple wrapper for JETM to provide a
 * more Scala-like usage.
 *
 * See http://jetm.void.fm/
 */
object SETM {
  BasicEtmConfigurator.configure(true) // nested
  private val monitor = EtmManager.getEtmMonitor()

  // Start monitoring
  monitor.start()


  def enable = monitor.enableCollection()
  def disable = monitor.disableCollection()

  /*
   * Stop monitoring, collect results and render them
   *
   * @param renderer The renderer to use
   */
  def collect() = {
    monitor.render(new SimpleTextRenderer())
    monitor.stop()
  }

  /**
   * Measure execution time of the given function
   *
   * @param name A human readable name to identify this timing measurement
   * @param f The function to measure execution time of
   */
  def timing[T](name: String)(f: => T) = {
    val p = monitor.createPoint(name)
    try {
      f
    } finally {
      p.collect
    }
  }
}

