package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.scribengin.storage.StorageConfig;
import com.neverwinterdp.scribengin.storage.hdfs.HDFSStorage;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSourcePartition;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSourcePartitionStream;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSourcePartitionStreamReader;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

public class VMTMValidatorHDFSApp extends VMApp {
  private Logger logger;
  
  @Override
  public void run() throws Exception {
    logger =  getVM().getLoggerFactory().getLogger(VMTMValidatorHDFSApp.class) ;
    logger.info("Start run()");
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    VMConfig vmConfig = vmDescriptor.getVmConfig();
    Registry registry = getVM().getVMRegistry().getRegistry();
    registry.setRetryable(true);
    
    String reportPath   = vmConfig.getProperty("tracking.report-path", "/applications/tracking-message");
    int    numOfReader  = vmConfig.getPropertyAsInt("tracking.num-of-reader", 3);
    long   maxRuntime   = vmConfig.getPropertyAsLong("tracking.max-runtime", 120000);
    int    expectNumOfMessagePerChunk = vmConfig.getPropertyAsInt("tracking.expect-num-of-message-per-chunk", 0);
    
    String hdfsLocation        = vmConfig.getProperty("hdfs.location", "/tracking-sample/hdfs/aggregate");
    long   partitionRollPeriod = vmConfig.getPropertyAsLong("hdfs.partition-roll-period", (15 * 60 * 1000));
    
    TrackingValidatorService validatorService = new TrackingValidatorService(registry, reportPath);
    validatorService.withExpectNumOfMessagePerChunk(expectNumOfMessagePerChunk);
    validatorService.addReader(
        new HDFSTrackingMessageReader(hdfsLocation, partitionRollPeriod)
    );
    validatorService.start();
    validatorService.awaitForTermination(maxRuntime, TimeUnit.MILLISECONDS);
  }

  public class HDFSTrackingMessageReader extends TrackingMessageReader {
    private BlockingQueue<TrackingMessage> queue = new LinkedBlockingQueue<>(5000);
    private long partitionRollPeriod ;
    private HDFSSourceReader hdfsSourceReader ;
    
    HDFSTrackingMessageReader(String hdfsLocation, long rollPeriod) {
      hdfsSourceReader = new HDFSSourceReader(hdfsLocation, rollPeriod) {
        @Override
        public void onTrackingMessage(TrackingMessage tMesg) throws Exception {
          queue.offer(tMesg);
        }
      };
    }
    
    public void onInit(TrackingRegistry registry) throws Exception {
      hdfsSourceReader.start();
    }
   
    public void onDestroy(TrackingRegistry registry) throws Exception{
      hdfsSourceReader.interrupt();
    }
    
    @Override
    public TrackingMessage next() throws Exception {
      return queue.poll(partitionRollPeriod + 300000, TimeUnit.MILLISECONDS);
    }
  }
  
  abstract public class HDFSSourceReader extends Thread {
    private String hdfsLocation ;
    private long   partitionRollPeriod;
    
    HDFSSourceReader(String hdfsLocation, long partitionRollPeriod) {
      this.hdfsLocation        = hdfsLocation;
      this.partitionRollPeriod = partitionRollPeriod;
    }
    
    abstract public void onTrackingMessage(TrackingMessage tMesg) throws Exception ;
    
    public void run() {
      try {
        doRun();
      } catch (Exception e) {
        logger.error("Error:", e);
      }
    }
    
    void doRun() throws Exception {
      StorageConfig storageConfig = new StorageConfig("HDFS", hdfsLocation);
      Configuration conf = new Configuration();
      VMConfig.overrideHadoopConfiguration(getVM().getDescriptor().getVmConfig().getHadoopProperties(), conf);
      FileSystem fs = FileSystem.get(conf);
      storageConfig.setPartitionStream(1);

      HDFSStorage hdfsStorage = new HDFSStorage(fs, storageConfig);
      HDFSSource hdfsSource = hdfsStorage.getSource();
      int noPartitionFound = 0 ;
      while(true) {
        List<HDFSSourcePartition> partitions = hdfsSource.getSourcePartitions();
        boolean validatePartition = false;
        if(partitions.size() > 0) {
          noPartitionFound = 0;
          HDFSSourcePartition partition = partitions.get(0);
          Date timestamp   = getTimestamp(partition.getPartitionLocation());
          Date currentTime = new Date();
          if(currentTime.getTime() > timestamp.getTime() + partitionRollPeriod) {
            validatePartition(partition);
            partition.delete();
            validatePartition = true;
          }
        } else {
          noPartitionFound++;
        }
        if(noPartitionFound > 10) break ;
        if(!validatePartition) Thread.sleep(60000);
      }
    }
    
    Date getTimestamp(String partitionLocation) throws ParseException {
      SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd@HH:mm");
      int index = partitionLocation.indexOf("storage-");
      String timestamp = partitionLocation.substring(index + "storage-".length());
      return timestampFormat.parse(timestamp);
    }
    
    void validatePartition(HDFSSourcePartition partition) throws Exception {
      HDFSSourcePartitionStream[] stream = partition.getPartitionStreams();
      for(int i = 0; i < stream.length; i++) {
        HDFSSourcePartitionStreamReader reader = stream[i].getReader("validator") ;
        Record record = null;
        while((record = reader.next(1000)) != null) {
          TrackingMessage tMesg = JSONSerializer.INSTANCE.fromBytes(record.getData(), TrackingMessage.class);
          onTrackingMessage(tMesg);
        }
      }
    }
  }
}