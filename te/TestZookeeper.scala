package te

import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient

object TestZookeeper {
  def main(args: Array[String]): Unit = {
    val topic : String = "tpp" //消费的 topic 名字
    // val topics : Set[String] = Set(topic) //创建 stream 时使用的 topic 名字集合
    val topicDirs = new ZKGroupTopicDirs("sst", topic)
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    val zkClient = new ZkClient("192.168.249.10:2181,192.168.249.11:2181")
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    println(children)
  }
}
