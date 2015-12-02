package com.neverwinterdp.storage.ssm.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.storage.ssm.SSM;
import com.neverwinterdp.storage.ssm.SSMReader;
import com.neverwinterdp.storage.ssm.SSMRegistry;
import com.neverwinterdp.storage.ssm.SSMRegistryPrinter;
import com.neverwinterdp.storage.ssm.SSMWriter;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HdfsSSM extends SSM {
  private FileSystem fs;
  private String     storageLocation;

  public HdfsSSM(FileSystem fs, String storageLoc, Registry registry, String regPath) throws RegistryException, IOException {
    this.fs              = fs;
    this.storageLocation = storageLoc;
    
    SSMRegistry segStorageReg = new SSMRegistry(registry, regPath);
    if(!registry.exists(regPath)) {
      segStorageReg.initRegistry();
    }
    init(segStorageReg);
    
    Path hdfsStoragePath = new Path(storageLoc);
    if(!fs.exists(hdfsStoragePath)) {
      fs.mkdirs(hdfsStoragePath);
    }
  }
  
  protected SSMWriter createWriter(String clientId, SSMRegistry registry) throws RegistryException{
    return new HdfsSSMWriter(clientId, registry, fs, storageLocation);
  }

  @Override
  protected SSMReader createReader(String clientId, SSMRegistry registry) throws RegistryException, IOException {
    return new HdfsSSMReader(clientId, registry, fs, storageLocation);
  }
  
  @Override
  public HdfsSSMConsistencyVerifier getSegmentConsistencyVerifier() {
    return new HdfsSSMConsistencyVerifier(registry, fs, storageLocation);
  }
  
  public void dump() throws RegistryException, IOException {
    SSMRegistryPrinter rPrinter = new SSMRegistryPrinter(System.out, registry);
    rPrinter.print();
    HDFSUtil.dump(fs, storageLocation);
  }
}
