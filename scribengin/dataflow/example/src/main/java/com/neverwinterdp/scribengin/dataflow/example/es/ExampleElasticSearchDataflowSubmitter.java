package com.neverwinterdp.scribengin.dataflow.example.es;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.dataflow.DataSet;
import com.neverwinterdp.scribengin.dataflow.Dataflow;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.KafkaDataSet;
import com.neverwinterdp.scribengin.dataflow.Operator;
import com.neverwinterdp.scribengin.dataflow.example.simple.SimpleDataStreamOperator;
import com.neverwinterdp.scribengin.shell.ScribenginShell;
import com.neverwinterdp.storage.es.ESStorageConfig;
import com.neverwinterdp.storage.kafka.KafkaStorageConfig;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.YarnVMClient;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ExampleElasticSearchDataflowSubmitter {
  /**
   * Simple class to house our configuration options for JCommander 
   */
  static public class Config {
    @Parameter(names = {"--help", "-h"}, help = true, description = "Output this help message")
    private boolean help;

    @Parameter(names = "--local-app-home", required=true, description="The example dataflow local location")
    String localAppHome ;
    
    @Parameter(names = "--dfs-app-home", description="DFS location to upload the example dataflow")
    String dfsAppHome = "/applications/dataflow/example";
    
    @Parameter(names = "--zk-connect", description="[host]:[port] of Zookeeper server")
    String zkConnect = "zookeeper-1:2181";
    
    @Parameter(names = "--hadoop-master-connect", description="Hostname of HadoopMaster")
    String hadoopMasterConnect = "hadoop-master";

    @Parameter(names = "--dataflow-id", description = "Unique ID for the dataflow")
    String dataflowId        = "ExampleDataflow";
    
    @Parameter(names = "--dataflow-default-replication", description = "Dataflow default replication")
    int dataflowDefaultReplication = 1;
    
    @Parameter(names = "--dataflow-default-parallelism", description = "The dataflow default parallelism")
    int dataflowDefaultParallelism = 8;
    
    @Parameter(names = "--dataflow-num-of-worker", description = "Number of workers to request")
    int dataflowNumOfWorker = 2;
    
    @Parameter(names = "--dataflow-num-of-executor-per-worker", description = "Number of Executors per worker to request")
    int dataflowNumOfExecutorPerWorker = 2;
    
    @Parameter(names = "--input-topic", description = "Name of input Kafka Topic")
    String inputTopic = "input.topic";
    
    @Parameter(names = "--input-num-of-messages", description = "Name of input Kafka Topic")
    int inputNumOfMessages = 10000;
    
    @Parameter(names = "--output-topic", description = "Name of output Kafka topic")
    String outputTopic ="output.topic";
    
    @Parameter(names = "--elasticsearch-host", description = "Location of Elasticsearch")
    String elasticSearchHost ="elasticsearch:9200";
    
    @Parameter(names = "--index", description = "Elasticsearch index")
    String index ="ScribeIndex";
    
    @Parameter(names = "--mapper", description = "Class to use as a mapper")
    String mapper ="com.neverwinterdp.storage.es.Mapper";
  }
  
  private Config config = new Config();
  
  private ScribenginShell shell;
  
  /**
   * Constructor - sets shell to access Scribengin and configuration properties 
   * @param shell ScribenginShell to connect to Scribengin with
   * @param props Properties to configure the dataflow
   */
  public ExampleElasticSearchDataflowSubmitter(ScribenginShell shell, Config config){
    this.shell = shell;
    this.config = config;
  }
  
  /**
   * The logic to submit the dataflow
   * @throws Exception
   */
  public void submitDataflow() throws Exception {
    //Upload our app to HDFS
    VMClient vmClient = shell.getScribenginClient().getVMClient();
    vmClient.uploadApp(config.localAppHome, config.dfsAppHome);
    
    Dataflow dfl = buildDataflow();
    //Get the dataflow's descriptor
    DataflowDescriptor dflDescriptor = dfl.buildDataflowDescriptor();
    //Output the descriptor in human-readable JSON
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor));

    //Ensure all your sources and sinks are up and running first, then...

    //Submit the dataflow and wait until it starts running
    DataflowSubmitter submitter = new DataflowSubmitter(shell.getScribenginClient(), dfl);
    submitter.submit().waitForDataflowRunning(60000);

    /** Wait for the dataflow to complete within the given timeout */
    //submitter.waitForDataflowStop(60000);
  }
  
  /**
   * The logic to build the dataflow configuration
   * @param kafkaZkConnect [host]:[port] of Kafka's Zookeeper conenction 
   * @return
   * @throws ClassNotFoundException 
   */
  public Dataflow buildDataflow() throws ClassNotFoundException{
    //Create the new Dataflow object
    // <Message,Message> pertains to the <input,output> object for the data
    Dataflow dfl = new Dataflow(config.dataflowId);
    dfl.
      setDFSAppHome(config.dfsAppHome).
      setDefaultParallelism(config.dataflowDefaultParallelism).
      setDefaultReplication(config.dataflowDefaultReplication);
    
    dfl.getWorkerDescriptor().setNumOfInstances(config.dataflowNumOfWorker);
    dfl.getWorkerDescriptor().setNumOfExecutor(config.dataflowNumOfExecutorPerWorker);
    
    
    //Define our input source - set name, ZK host:port, and input topic name
    KafkaDataSet<Message> inputDs = 
      dfl.createInput(new KafkaStorageConfig("input", config.zkConnect, config.inputTopic));
    
    
    ESStorageConfig esStorageConfig = new ESStorageConfig("output", config.elasticSearchHost, config.index, config.mapper);
    DataSet<Message> outputDs = dfl.createOutput(esStorageConfig);
    
    //Define which operator to use.  
    //This will be the logic that ties the input to the output
    Operator operator = dfl.createOperator("simpleOperator", SimpleDataStreamOperator.class);
    
    //Connect your input to the operator
    inputDs.useRawReader().connect(operator);
    //Connect your operator to the output
    operator.connect(outputDs);

    return dfl;
  }
  
  /**
   * Push data to Kafka
   * @param kafkaConnect Kafka's [host]:[port]
   * @param inputTopic Topic to write to
   * @throws Exception 
   */
  public void createInputMessages() throws Exception {
    KafkaTool kafkaTool = new KafkaTool("KafkaTool", config.zkConnect);
    String kafkaBrokerConnects = kafkaTool.getKafkaBrokerList();
    Properties props = new Properties();
    props.put("metadata.broker.list", kafkaBrokerConnects);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
    props.put("request.required.acks", "1");
    props.put("retry.backoff.ms", "1000");
    ProducerConfig producerConfig = new ProducerConfig(props);
    
    Producer<String, String> producer = new Producer<String, String>(producerConfig);
    for(int i = 0; i < config.inputNumOfMessages; i++){
      String messageKey = "key-" + i;
      String message    = Integer.toString(i);
      producer.send(new KeyedMessage<String, String>(config.inputTopic, messageKey, message));
      if((i + 1) % 500 == 0) {
        System.out.println("Send " + (i + 1) + " messages");
      }
    }
    producer.close();
  }
  
  /**
   * Reads in from a Kafka topic and ensures all messages have been consumed
   * @return True if test passes, False otherwise
   * @throws Exception
   */
  public boolean validate() throws Exception {
    ConsumerIterator<byte[], byte[]> it = getConsumerIterator(config.zkConnect, config.outputTopic);
    int[] output = new int[config.inputNumOfMessages];
    Arrays.fill(output, -1);
    int count = 0;
    try {
      while(it.hasNext()) {
        Message message = JSONSerializer.INSTANCE.fromBytes(it.next().message(), Message.class);
        String data = new String(message.getData());
        int value = Integer.parseInt(data);
        output[value] = value;
        count++ ;
        if(count % 500 == 0) {
          System.out.println("Read " + count + " messages");
        }
      }
    } catch (ConsumerTimeoutException e) { 
     //e.printStackTrace();
    }
    
    if(count != config.inputNumOfMessages) {
      throw new Exception("Input " + config.inputNumOfMessages + ", but can read only " + config.inputNumOfMessages + " messages");
    }
    
    for(int i = 0; i < output.length; i++) {
      if(i != output[i]) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Get Kafka Consumer for topic
   * @param zkConnect Zookeeper [host]:[port]
   * @param topic Topic to read from
   * @return
   */
  private ConsumerIterator<byte[], byte[]> getConsumerIterator(String zkConnect, String topic){
    Properties props = new Properties();
    props.put("zookeeper.connect", zkConnect);
    props.put("group.id", "default");
    props.put("consumer.timeout.ms", "5000");
    props.put("auto.offset.reset", "smallest");
    
    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
    return stream.iterator();
  }
  
  static public void main(String args[]) throws Exception {
    //Use JCommander to parse command line args
    Config config = new Config();
    JCommander jCommander = new JCommander(config, args);
    
    if (config.help) {
      jCommander.usage();
      return;
    }
    
    //Create a registry configuration and point it to our running Registry (Zookeeper)
    RegistryConfig registryConfig = RegistryConfig.getDefault();
    registryConfig.setConnect(config.zkConnect);
    Registry registry = null;
    try{
      registry = new RegistryImpl(registryConfig).connect();
    } catch(Exception e){
      System.err.println("Could not connect to the registry at: "+ registryConfig.getConnect()+"\n"+e.getMessage());
      return;
    }
    
    //Configure where our hadoop master lives
    String hadoopMaster = config.hadoopMasterConnect;
    HadoopProperties hadoopProps = new HadoopProperties() ;
    hadoopProps.put("yarn.resourcemanager.address", hadoopMaster + ":8032");
    hadoopProps.put("fs.defaultFS", "hdfs://" + hadoopMaster +":9000");
    
    //Set up our connection to Scribengin
    YarnVMClient vmClient = new YarnVMClient(registry, VMConfig.ClusterEnvironment.YARN, hadoopProps) ;
    ScribenginShell shell = new ScribenginShell(vmClient) ;
    shell.attribute(HadoopProperties.class, hadoopProps);
    
    ExampleElasticSearchDataflowSubmitter simpleDataflowExample = new ExampleElasticSearchDataflowSubmitter(shell, config);
    //Create input data for our dataflow to consume
    simpleDataflowExample.createInputMessages();
    //Launch our configured dataflow
    simpleDataflowExample.submitDataflow();
    
    //Wait to make sure that dataflow is running and produce some messages to the output topic
    Thread.sleep(1500);
    //Validate all messages have been consumed
    simpleDataflowExample.validate();
    
    //Get some info on the running dataflow
    shell.execute("dataflow info --dataflow-id " + config.dataflowId);
    
    //Close connection with Scribengin
    shell.close();
    shell.console().println("Simple Example Datafow is done!!!");
    System.exit(0);
  }
  
  public String getDataflowID() { return config.dataflowId; }

  public String getInputTopic() { return config.inputTopic; }
}
