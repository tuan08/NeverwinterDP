package com.neverwinterdp.kafka.tool;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;

public class KafkaTopicCheckToolUnitTest {
  static private KafkaCluster cluster;

  @BeforeClass
  static public void setUp() throws Exception {
    LoggerFactory.log4jUseConsoleOutputConfig("WARN");
    FileUtil.removeIfExist("./build/cluster", false);
    cluster = new KafkaCluster("./build/cluster", 1, 2);
    cluster.start();
    Thread.sleep(5000);
  }
  
  @AfterClass
  static public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testTool() throws Exception {
    String[] args = {
      "--zk-connect", cluster.getZKConnect(),
      "--num-partition", "3",
      "--replication", "1",
      "--send-period", "0",
      "--send-message-size", "500",
      "--send-max-per-partition", "1000",
      "--send-max-duration", "60000",
      "--send-writer-type", "ack",
      "--junit-report", "build/junit-reports/kafkaTopicCheckToolUnitTest.xml"
    };
    KafkaTopicCheckTool.main(args);
  }
}
