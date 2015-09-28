package com.neverwinterdp.scribengin.tool;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.kafka.tool.server.KafkaCluster;
import com.neverwinterdp.module.AppServiceModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.vm.client.LocalVMClient;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.tool.VMClusterBuilder;

public class EmbededVMClusterBuilder extends VMClusterBuilder {
  private String baseDir = "./build/cluster";
  protected KafkaCluster kafkaCluster;
  
  public EmbededVMClusterBuilder() throws RegistryException {
    this(new LocalVMClient());
  }
  
  public EmbededVMClusterBuilder(VMClient vmClient) throws RegistryException {
    super(null, vmClient);
  }
  
  public EmbededVMClusterBuilder(String baseDir, VMClient vmClient) {
    super(null, vmClient);
    this.baseDir = baseDir ;
  }
  
  @Override
  public void clean() throws Exception {
    super.clean(); 
    FileUtil.removeIfExist(baseDir, false);
  }
  
  @Override
  public void start() throws Exception {
    startKafkaCluster() ;
    super.start();
  }
  
  public void startKafkaCluster() throws Exception {
    h1("Start kafka cluster");
    kafkaCluster = new KafkaCluster(baseDir, 1, 1);
    kafkaCluster.setNumOfPartition(5);
    kafkaCluster.start();
    Thread.sleep(1000);
  }
  
  @Override
  public void shutdown() throws Exception {
    super.shutdown();
    kafkaCluster.shutdown();
  }

  public <T> Injector newAppContainer() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("registry.connect", "127.0.0.1:2181") ;
    props.put("registry.db-domain", "/NeverwinterDP") ;
    
    props.put("implementation:" + Registry.class.getName(), RegistryImpl.class.getName()) ;
    AppServiceModule module = new AppServiceModule(props) ;
    return Guice.createInjector(module);
  }
  
  public RegistryConfig getRegistryConfig() { return vmClient.getRegistry().getRegistryConfig() ; }
  
  public Registry newRegistry() {
    return new RegistryImpl(getRegistryConfig());
  }
}
