package com.neverwinterdp.scribengin.storage.kafka.source;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.PartitionStreamConfig;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.source.SourcePartition;
import com.neverwinterdp.scribengin.storage.source.SourcePartitionStream;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

public class KafkaSourcePartition implements SourcePartition {
  private KafkaClient                     kafkaClient;
  private StorageConfig                   storageConfig;
  private Map<Integer, KafkaSourceStream> sourceStreams = new HashMap<Integer, KafkaSourceStream>();

  public KafkaSourcePartition(KafkaClient kafkaClient, StorageConfig sconfig) throws Exception {
    this.kafkaClient = kafkaClient;
    init(sconfig);
  }
  
  void init(StorageConfig sconfig) throws Exception {
    this.storageConfig = sconfig;
    TopicMetadata topicMetdadata = kafkaClient.findTopicMetadata(sconfig.attribute("topic"));
    List<PartitionMetadata> partitionMetadatas = topicMetdadata.partitionsMetadata();
    for(int i = 0; i < partitionMetadatas.size(); i++) {
      PartitionMetadata partitionMetadata = partitionMetadatas.get(i);
      KafkaSourceStream sourceStream = new KafkaSourceStream(kafkaClient, sconfig, partitionMetadata);
      sourceStreams.put(sourceStream.getId(), sourceStream);
    }
  }
  
  @Override
  public StorageConfig getStorageConfig() { return storageConfig; }

  /**
   * The stream id is equivalent to the partition id of the kafka
   */
  @Override
  public SourcePartitionStream getPartitionStream(int id) {  
    SourcePartitionStream stream = sourceStreams.get(id); 
    if(stream == null) {
      throw new RuntimeException("Cannot find the partition " + id + ", available streams = " + sourceStreams.size());
    }
    return stream;
  }

  @Override
  public SourcePartitionStream getPartitionStream(PartitionStreamConfig descriptor) {
    return getPartitionStream(descriptor.getPartitionStreamId());
  }

  @Override
  public SourcePartitionStream[] getPartitionStreams() {
    SourcePartitionStream[] array = new SourcePartitionStream[sourceStreams.size()];
    return sourceStreams.values().toArray(array);
  }
  
  public void close() throws Exception {
  }
}