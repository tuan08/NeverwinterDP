package net.tuan08.tracking.inmem;

import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.util.log.LoggerFactory;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServer;

import net.tuan08.tracking.TrackingGeneratorService;
import net.tuan08.tracking.TrackingMessageReport;
import net.tuan08.tracking.TrackingRegistry;
import net.tuan08.tracking.TrackingValidatorService;

import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;

public class TrackingMessageUnitTest {
  final static String WORKING_DIR           = "./build/working";
  final static String TRACKING_PATH         = "/tracking";
  final static int    NUM_OF_CHUNK          = 10;
  final static int    NUM_OF_MESG_PER_CHUNK = 500;
  
  private EmbededZKServer          zkServerLauncher;
  private Registry                 registry;
  private InMemTrackingStorage        trackingMessageDB;
  private TrackingGeneratorService generatorService;
  private TrackingValidatorService validatorService;
  
  @Before
  public void setup() throws Exception {
    LoggerFactory.log4jUseConsoleOutputConfig("WARN");
    FileUtil.removeIfExist(WORKING_DIR, false);
    zkServerLauncher = new EmbededZKServer( WORKING_DIR + "/zookeeper") ;
    zkServerLauncher.start();
    registry =  new RegistryImpl(RegistryConfig.getDefault()).connect() ;
    
    trackingMessageDB = new InMemTrackingStorage();
    
    generatorService  = new TrackingGeneratorService(registry, TRACKING_PATH, NUM_OF_CHUNK, NUM_OF_MESG_PER_CHUNK);
    generatorService.addWriter(new InMemTrackingStorage.Writer(trackingMessageDB));
    
    validatorService = new TrackingValidatorService(registry, TRACKING_PATH, NUM_OF_MESG_PER_CHUNK);
    validatorService.addReader(new InMemTrackingStorage.Reader(trackingMessageDB));
  }
  
  @After
  public void teardown() throws RegistryException {
    registry.shutdown();
    zkServerLauncher.shutdown();
  }
  
  @Test
  public void testTracking() throws Exception {
    generatorService.start();
    validatorService.start();
    
    generatorService.awaitForTermination(10000, TimeUnit.MILLISECONDS);
    generatorService.shutdown();

    validatorService.awaitForTermination(10000, TimeUnit.MILLISECONDS);
    validatorService.shutdown();
    
    TrackingRegistry trackingRegistry = generatorService.getTrackingRegistry();
    List<TrackingMessageReport> generatedReports = trackingRegistry.getGeneratorReports();
    List<TrackingMessageReport> validatedReports = trackingRegistry.getValidatorReports();
    System.out.println(TrackingMessageReport.getFormattedReport("Generated Report", generatedReports));
    System.out.println(TrackingMessageReport.getFormattedReport("Validated Report", validatedReports));
  }
}