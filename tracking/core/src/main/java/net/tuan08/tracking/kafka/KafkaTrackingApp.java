package net.tuan08.tracking.kafka;

import java.util.concurrent.TimeUnit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;

import net.tuan08.tracking.TrackingGeneratorService;
import net.tuan08.tracking.TrackingValidatorService;

public class KafkaTrackingApp {
  @Parameter(names = "--zk-connect", description = "The zk connect string")
  private String zkConnects = "localhost:2181";
  
  @Parameter(names = "--tracking-path", description = "The zk connect string")
  private String trackingPath = "/tracking";
  
  @Parameter(names = "--num-of-chunk", description = "The number of chunks")
  private int numOfChunk = 10;
  
  @Parameter(names = "--num-of-message-per-chunk", description = "The number of messages per chunk")
  private int numOfMessagePerChunk = 100;
  
  @Parameter(names = "--input-topic", description = "The input topic")
  private String inputTopic = "tracking";

  @Parameter(names = "--num-of-partition", description = "The number of the partitions")
  private int    numOfPartition = 5;
  
  @Parameter(names = "--num-of-replication", description = "The number of the replications")
  private int    numOfReplication = 1;
  
  @Parameter(names = "--output-topic", description = "The input topic")
  private String outputTopic = "tracking";
  
  @Parameter(names = "--max-run-time", description = "The max run time for the application")
  private long maxRunTime = 25000;
  
  private Registry registry ;
  private TrackingGeneratorService generatorService;
  private TrackingValidatorService validatorService;
  
  public KafkaTrackingApp(String[] args) throws Exception {
    new JCommander(this, args);
    RegistryConfig regConfig = RegistryConfig.getDefault();
    regConfig.setConnect(zkConnects);
    registry =  new RegistryImpl(regConfig).connect() ;
    
    generatorService  = new TrackingGeneratorService(registry, trackingPath, numOfChunk, numOfMessagePerChunk);
    String[] writerConfig = {
        "--zk-connect", zkConnects, 
        "--topic", inputTopic,
        "--num-of-partition", Integer.toString(numOfPartition),
        "--num-of-replication", Integer.toString(numOfReplication)
    };
    generatorService.addWriter(new KafkaTrackingWriter(writerConfig));
    
    validatorService = new TrackingValidatorService(registry, trackingPath, numOfMessagePerChunk);
    String[] readerConfig = {
      "--zk-connect", zkConnects,
      "--topic",      outputTopic,
    };
    validatorService.addReader(new KafkaTrackingReader(readerConfig)); 
  }
  
  public void start() throws Exception {
    generatorService.start();
    validatorService.start();
  }
  
  public void waitForTermination() throws Exception {
    long startTime = System.currentTimeMillis() ;
    generatorService.awaitForTermination(maxRunTime, TimeUnit.MILLISECONDS);
    generatorService.shutdown();
    long duration = System.currentTimeMillis() - startTime; 
    
    if(maxRunTime - duration > 0) {
      validatorService.awaitForTermination(maxRunTime - duration, TimeUnit.MILLISECONDS);
      validatorService.shutdown();
    } else {
      validatorService.shutdown();
    }
  }
  
  public void shutdown() throws Exception {
    registry.shutdown();
  }
  
  public void run() throws Exception {
    start();
    System.err.println("pass start");
    waitForTermination();
    System.err.println("pass wait for termination");
    shutdown();
    System.err.println("pass shutdown");
  }
  
  static public void main(String[] args) throws Exception {
    KafkaTrackingApp app = new KafkaTrackingApp(args);
    app.run();
  }
}
