package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.mycila.guice.ext.closeable.CloseableInjector;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.es.log.OSMonitorLoggerService;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.module.MycilaJmxModuleExt;
import com.neverwinterdp.os.RuntimeEnv;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.yara.MetricPrinter;
import com.neverwinterdp.yara.MetricRegistry;


public class VMDataflowWorkerApp extends VMApp {
  private Logger logger  ;
  
  private DataflowContainer container;
  private DataflowTaskExecutorService dataflowTaskExecutorService;
  
  @Override
  public void run() throws Exception {
    final VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    logger = getVM().getLoggerFactory().getLogger(VMDataflowWorkerApp.class);
    AppModule module = new AppModule(vmConfig.getProperties()) {
      @Override
      protected void configure(Map<String, String> properties) {
        bindInstance(RuntimeEnv.class, getVM().getRuntimeEnv());
        bindInstance(MetricRegistry.class, new MetricRegistry(vmConfig.getName()));
        bindInstance(LoggerFactory.class, getVM().getLoggerFactory());
        Registry registry = getVM().getVMRegistry().getRegistry();
        bindInstance(RegistryConfig.class, registry.getRegistryConfig());
        bindType(Registry.class, registry.getClass());
        bindInstance(VMDescriptor.class, getVM().getDescriptor());
        try {
          VMConfig.Environment env = vmConfig.getEnvironment();
          if(env == VMConfig.Environment.YARN || env == VMConfig.Environment.YARN_MINICLUSTER) {
            YarnConfiguration conf = new YarnConfiguration();
            vmConfig.overrideHadoopConfiguration(conf);
            bindInstance(FileSystem.class, FileSystem.get(conf));
          } else {
            bindInstance(FileSystem.class, FileSystem.getLocal(new Configuration()));
          }
        } catch (Exception e) {
          logger.error("Error:", e);;
        }
      };
    };
    Module[] modules = {
      new CloseableModule(),new Jsr250Module(), 
      new MycilaJmxModuleExt(getVM().getDescriptor().getVmConfig().getName()), 
      module
    };
    Injector injector = Guice.createInjector(Stage.PRODUCTION, modules);
    container = injector.getInstance(DataflowContainer.class);
    container.getDataflowRegistry().addWorker(getVM().getDescriptor());
    dataflowTaskExecutorService = container.getDataflowTaskExecutorManager();
    addListener(new VMApp.VMAppTerminateEventListener() {
      @Override
      public void onEvent(VMApp vmApp, TerminateEvent terminateEvent) {
        try {
          if(terminateEvent == TerminateEvent.Shutdown) {
            dataflowTaskExecutorService.shutdown();
          } else if(terminateEvent == TerminateEvent.SimulateKill) {
            dataflowTaskExecutorService.simulateKill();
          } else if(terminateEvent == TerminateEvent.Kill) {
            System.exit(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    //TODO: fix to use module
    container.getInstance(OSMonitorLoggerService.class);
    try {
      dataflowTaskExecutorService.start();
      dataflowTaskExecutorService.waitForTerminated(500);
    } catch(InterruptedException ex) {
    } finally {
      MetricPrinter metricPrinter = new MetricPrinter(System.out) ;
      MetricRegistry mRegistry = container.getInstance(MetricRegistry.class);
      metricPrinter.print(mRegistry);
      container.getInstance(CloseableInjector.class).close();
    }
  }
}