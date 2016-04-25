package net.tuan08.tracking.kafka;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.KafkaAdminTool;
import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.util.JSONSerializer;

import net.tuan08.tracking.TrackingMessage;
import net.tuan08.tracking.TrackingRegistry;
import net.tuan08.tracking.TrackingWriter;

public class KafkaTrackingWriter extends TrackingWriter {
  @Parameter(names = "--zk-connect", description = "The zk connect string")
  private String zkConnects = "localhost:2181";
  
  @Parameter(names = "--topic", description = "The input topic")
  private String topic = "tracking";
  
  @Parameter(names = "--num-of-partition", description = "The number of the partitions")
  private int    numOfPartition = 5;
  
  @Parameter(names = "--num-of-replication", description = "The number of the replications")
  private int    numOfReplication = 1;
  
  private AckKafkaWriter    kafkaWriter;
  
  public KafkaTrackingWriter(String[] args) throws Exception {
    new JCommander(this, args);
  }
  
  public void onInit(TrackingRegistry registry) throws Exception {
    KafkaAdminTool kafkaAdminTool = new KafkaAdminTool("KafkaAdminTool", zkConnects);
    if(!kafkaAdminTool.topicExits(topic)) {
      kafkaAdminTool.createTopic(topic, numOfReplication, numOfPartition);
    }
    KafkaTool kafkaTool = new KafkaTool("KafkaTool", zkConnects);
    kafkaWriter = new AckKafkaWriter("KafkaLogWriter", kafkaTool.getKafkaBrokerList()) ;
    kafkaTool.close();
  }
 
  public void onDestroy(TrackingRegistry registry) throws Exception{
    kafkaWriter.close();
  }
  
  @Override
  public void write(TrackingMessage message) throws Exception {
    String json = JSONSerializer.INSTANCE.toString(message);
    kafkaWriter.send(topic, json, 30 * 1000);
  }
  
  
}