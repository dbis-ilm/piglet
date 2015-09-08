package dbis.pfabric.deploy.util
import java.util.Properties

import kafka.utils._
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import scala.collection._
import scala.collection.JavaConversions._
import kafka.cluster.Broker
import kafka.log.LogConfig
import kafka.consumer.Whitelist
import kafka.server.OffsetManager
import org.apache.kafka.common.utils.Utils.formatAddress
import kafka.common.{ Topic, AdminCommandFailedException }
object ZkClientConfig {
  val sessionTimeout: Int = 30000
  val connectTimeout: Int = 30000
}

class KafkaManager(zkClientPtah: String) {
  val zkClient = new ZkClient(zkClientPtah, ZkClientConfig.connectTimeout, ZkClientConfig.sessionTimeout, ZKStringSerializer)
  def createTopic(topic: String, partitions: Int) {
    val configs = new Properties
    AdminUtils.createTopic(zkClient, topic, partitions, 1, configs)
    println(s"Created topic $topic")
  }

  def topicExists(topic: String) = AdminUtils.topicExists(zkClient, topic)

  def listTopics(): Seq[String] = {
    val topics = ZkUtils.getAllTopics(zkClient).sorted
    for (topic <- topics) {
      if (ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic))) {
        println(s"$topic - marked for deletion")
      } else {
        println(topic)
      }
    }
    topics
  }

  def deleteAllTopics() {
    val zkClient = new ZkClient(zkClientPtah, ZkClientConfig.connectTimeout, ZkClientConfig.sessionTimeout, ZKStringSerializer)
    val topics = ZkUtils.getAllTopics(zkClient).sorted
    for (topic <- topics) {
      deleteTopic(topic)
    }
  }

  def deleteTopic(topic: String) {
    zkClient.deleteRecursive(ZkUtils.getTopicPath(topic))
  }

  def addTopicPartition(topic: String,
                        numPartitions: Int = 1,
                        replicaAssignmentStr: String = "",
                        checkBrokerAvailable: Boolean = true,
                        config: Properties = new Properties) {
    AdminUtils.addPartitions(zkClient, topic, numPartitions, replicaAssignmentStr, checkBrokerAvailable, config)
  }

  def describeAllTopics() {
    val topics = listTopics
    for (topic <- topics) {
      describeTopic(topic)
    }
  }

  def describeTopic(topic: String) {
    ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic)).get(topic) match {
      case Some(topicPartitionAssignment) =>
        val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
        val configs = AdminUtils.fetchTopicConfig(zkClient, topic)
        val partitionsCnt = topicPartitionAssignment.size
        val replFactor = topicPartitionAssignment.head._2.size
        println(s"Topic: $topic  PartitionCount:$partitionsCnt ReplicationFactor: $replFactor Configs:${configs.map(kv => kv._1 + "=" + kv._2).mkString(",")}")
        for ((partitionId, assignedReplicas) <- sortedPartitions) {
          val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionId)
          val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partitionId)
          print("\tTopic: " + topic)
          print("\tPartition: " + partitionId)
          print("\tLeader: " + (if (leader.isDefined) leader.get else "none"))
          print("\tReplicas: " + assignedReplicas.mkString(","))
          println("\tIsr: " + inSyncReplicas.mkString(","))
        }
      case None =>
        println("Topic " + topic + " doesn't exist!")

    }
  }

}