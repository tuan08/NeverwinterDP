package com.neverwinterdp.kafka.producer;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaMessageSendTool;
import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.util.text.TabularFormater;

import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.PartitionMetadata;

/**
 * This unit test is used to isolate and show all the kafka producer bugs and limitation. The following scenarios are tested 
 * @author Tuan
 */

public class KafkaProducerPartitionLeaderChangeBugUnitTest  {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }
  private KafkaCluster cluster ;
  
  @Before
  public void setup() throws Exception {
    cluster = new KafkaCluster("./build/kafka", 1, 4);
    cluster.setNumOfPartition(5);
    cluster.start();
  }
  
  @After
  public void teardown() throws Exception {
    cluster.shutdown();
  }
  
  @Test
  public void testPartitionLeaderChange() throws Exception {
    String TOPIC = "test";
  //create topic on brokers 1,2
    String[] args = { 
      "--create", "--topic", TOPIC, "--zookeeper", cluster.getZKConnect(), "--replica-assignment", "1:2,2:1"
      //"--partitions", "2", "--replication-factor", "2" parameters have no effect since --replica-assignment has higher priority 
    };
    KafkaTool kafkaClient = new KafkaTool("KafkaTool", cluster.getZKConnect());
    kafkaClient.getKafkaAdminTool().createTopic(args);
    info(kafkaClient.findTopicMetadata(TOPIC).partitionsMetadata());

    String[] sendArgs = {
      "--topic" , TOPIC, "--num-partition", "3", "--replication", "2", 
      "--send-writer-type", "default", "--send-period", "0", "--send-max-per-partition", "10000", "--send-max-duration", "60000"
    };
    
    KafkaMessageSendTool sendTool = new KafkaMessageSendTool(sendArgs);
    sendTool.runAsDeamon();

    while(sendTool.getSentCount() <= 0) Thread.sleep(50); //wait to make sure that send tool send some messages
    kafkaClient.getKafkaAdminTool().reassignPartitionReplicas(TOPIC, 0, 4, 3, 2);
    //It seems that we do not instruct to change the leader, we loose more messages ?
    kafkaClient.getKafkaAdminTool().moveLeaderToPreferredReplica(TOPIC, 0);
    Thread.sleep(500);
    info(kafkaClient.findTopicMetadata(TOPIC).partitionsMetadata());
    
    sendTool.waitForTermination();
    
    String[] checkArgs = { 
      "--topic", TOPIC,
      "--consume-max", Long.toString(sendTool.getSentCount()),
      "--zk-connect", cluster.getZKConnect()
    };
    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool(checkArgs);
    checkTool.run();
    System.out.println("Sent count = " + sendTool.getSentCount());
    System.out.println("Sent failed count = " + sendTool.getSentFailedCount());
    System.out.println("Check count = " + checkTool.getMessageCounter().getTotal());
    //TODO: review, since version 0.9.0.0 this problem has been fixed, in the previous version,
    //      the condition should be checkTool.getMessageCounter().getTotal() < sendTool.getSentCount()
    Assert.assertTrue(checkTool.getMessageCounter().getTotal() <= sendTool.getSentCount());
  }
  
  private void info(List<PartitionMetadata> holder) {
    String[] header = { "Partition Id", "Leader", "Replicas" };
    TabularFormater formater = new TabularFormater(header);
    formater.setTitle("Partitions");
    for(PartitionMetadata sel : holder) {
      StringBuilder replicas = new StringBuilder();
      for(BrokerEndPoint broker : sel.replicas()) {
        if(replicas.length() > 0) replicas.append(",");
        replicas.append(broker.port());
      }
      formater.addRow(sel.partitionId(), sel.leader().port(), replicas.toString());
    }
    System.out.println(formater.getFormatText());
  }
}