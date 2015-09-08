package dbis.pfabric.deploy.yarn.appmaster
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerStatus
/**
 * Classes or Listeners that want to listen to callback events from the application master
 * must implement this interface.
 */
trait AppMasterListener {
  /**
   * If the value is true, the application master will refrain to poll the resource manager.
   * Hence, onShutdown will be called.
   */
  def shouldShutdown: Boolean = false

  /**
   * Before entering the resource manager polling event loop, this  callback is invoked by 
   * the application master.
   */
  def onInit() {}

  /**
   * When the resource manager responds with a reboot request, this callback is invoked.
   */
  def onReboot() {}

  /**
   * When the application master has exited the resource manager polling event loop, and
   * is about to exit, this callback is invoked.
   */
  def onShutdown() {}

  /**
   * When the resource manager allocates a container for the application master, this
   * callback is invoked.
   * @param container the container to be allocated by the resource manager
   */
  def onContainerAllocated(container: Container) {}

  /**
   * When a container completes (either failure, or success), this callback
   * will be invoked.
   */
  def onContainerCompleted(status: ContainerStatus) {}

}