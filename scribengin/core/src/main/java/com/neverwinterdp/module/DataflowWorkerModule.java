package com.neverwinterdp.module;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.inject.name.Names;
import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.vm.VMConfig;



@ModuleConfig(name = "DataflowWorkerModule", autoInstall = false, autostart = false) 
public class DataflowWorkerModule extends ServiceModule {
  final static public String NAME = "DataflowWorkerModule" ;
  
  @Override
  protected void configure(Map<String, String> props) {  
    Names.bindProperties(binder(), props) ;
    try {
      Configuration conf = new Configuration();
      VMConfig.overrideHadoopConfiguration(props, conf);
      
      FileSystem fs = FileSystem.get(conf);
      bindInstance(FileSystem.class, fs);
      
      String kafkaZkConnects = props.get("kafka.zk.connects");
      KafkaClient kafkaClient = new KafkaClient("KafkaClient", kafkaZkConnects);
      bindInstance(KafkaClient.class, kafkaClient);
      
      S3Client s3Client = new S3Client();
      bindInstance(S3Client.class, s3Client);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}