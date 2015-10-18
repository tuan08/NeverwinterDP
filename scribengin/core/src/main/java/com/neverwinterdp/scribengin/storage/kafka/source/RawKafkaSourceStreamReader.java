package com.neverwinterdp.scribengin.storage.kafka.source;

import java.nio.ByteBuffer;

import kafka.javaapi.PartitionMetadata;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.source.CommitPoint;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class RawKafkaSourceStreamReader implements SourceStreamReader {
  private StreamDescriptor descriptor;
  private KafkaPartitionReader partitionReader ;
  private CommitPoint lastCommitInfo ;
  
  public RawKafkaSourceStreamReader(KafkaClient kafkaClient, StreamDescriptor descriptor, PartitionMetadata pMetadata) throws Exception {
    this.descriptor = descriptor;
    this.partitionReader = 
        new KafkaPartitionReader(descriptor.attribute("name"), kafkaClient, descriptor.attribute("topic"), pMetadata);
  }
  
  @Override
  public String getName() { return descriptor.attribute("name"); }

  @Override
  public DataflowMessage next(long maxWait) throws Exception {
    MessageAndOffset messageAndOffset = partitionReader.nextMessageAndOffset(maxWait) ;
    if(messageAndOffset == null) return null ;
    Message message = messageAndOffset.message();
    ByteBuffer payload = message.payload();
    byte[] messageBytes = new byte[payload.limit()];
    payload.get(messageBytes);
    
    ByteBuffer key = message.key();
    byte[] keyBytes = new byte[key.limit()];
    key.get(keyBytes);
    DataflowMessage dataflowMessage = new DataflowMessage(new String(keyBytes), messageBytes) ;
    return dataflowMessage;
  }

  @Override
  public DataflowMessage[] next(int size, long maxWait) throws Exception {
    throw new Exception("To implement") ;
  }
  
  public boolean isEndOfDataStream() { return false; }

  @Override
  public void rollback() throws Exception {
    throw new Exception("To implement") ;
  }

  @Override
  public void prepareCommit() throws Exception {
    //TODO: implement 2 phases commit correctly
  }

  @Override
  public void completeCommit() throws Exception {
    //TODO: implement 2 phases commit correctly
    partitionReader.commit();
  }
  
  @Override
  public void commit() throws Exception {
    try {
      prepareCommit() ;
      completeCommit() ;
    } catch(Exception ex) {
      rollback();
      throw ex;
    }
  }
  
  public CommitPoint getLastCommitInfo() { return this.lastCommitInfo ; }
  
  @Override
  public void close() throws Exception {
    if(partitionReader != null) {
      partitionReader.close();
    }
  }
}
