package com.neverwinterdp.scribengin.storage.sink;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.es.sink.ESSink;
import com.neverwinterdp.scribengin.storage.hdfs.sink.HDFSSink;
import com.neverwinterdp.scribengin.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;

@Singleton
public class SinkFactory {
  @Inject
  private FileSystem fs;
  
  @Inject
  private S3Client s3Client;
  
  @Inject
  private KafkaClient kafkaClient ;
  
  public SinkFactory() {
  }
  
  public SinkFactory(FileSystem fs) {
    this.fs = fs;
  }
  
  public SinkFactory(S3Client s3Client) {
    this.s3Client = s3Client;
  }
  
  public FileSystem getFileSyztem() { return this.fs ; }

  public Sink create(StorageDescriptor descriptor) throws Exception {
    if ("hdfs".equalsIgnoreCase(descriptor.getType())) {
      return new HDFSSink(fs, descriptor);
    } else if ("kafka".equalsIgnoreCase(descriptor.getType())) {
      return new KafkaSink(kafkaClient, descriptor);
    } else if ("s3".equalsIgnoreCase(descriptor.getType())) {
      return new S3Sink(s3Client, descriptor);
    } else if ("elasticsearch".equalsIgnoreCase(descriptor.getType())) {
      return new ESSink(descriptor);
    }
    throw new Exception("Unknown sink type " + descriptor.getType());
  }
  
  public Sink create(StreamDescriptor descriptor) throws Exception {
    if ("hdfs".equalsIgnoreCase(descriptor.getType())) {
      return new HDFSSink(fs, descriptor);
    } else if ("kafka".equalsIgnoreCase(descriptor.getType())) {
      return new KafkaSink(kafkaClient, descriptor);
    } else if ("s3".equalsIgnoreCase(descriptor.getType())) {
      return new S3Sink(s3Client, descriptor);
    } else if ("elasticsearch".equalsIgnoreCase(descriptor.getType())) {
      return new ESSink(descriptor);
    }
    throw new Exception("Unknown sink type " + descriptor.getType());
  }
}