package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.util.text.StringUtil;

public class LoggerConfig {
  @Parameter(names = "--log-level", description = "The log level")
  String   logLevel = "INFO ";

  @ParametersDelegate
  ConsoleAppender consoleAppender = new ConsoleAppender() ;

  
  @ParametersDelegate
  FileAppender fileAppender = new FileAppender() ;
  
  @ParametersDelegate
  ESAppender esAppender = new ESAppender() ;

  public String getLogLevel() {  return logLevel; }
  public void setLogLevel(String logLevel) { this.logLevel = logLevel; }

  public ConsoleAppender getConsoleAppender() { return consoleAppender; }
  public void setConsoleAppender(ConsoleAppender consoleAppender) { this.consoleAppender = consoleAppender; }

  public FileAppender getFileAppender() { return fileAppender; }
  public void setFileAppender(FileAppender fileAppender) { this.fileAppender = fileAppender; }

  public ESAppender getEsAppender() { return esAppender; }
  public void setEsAppender(ESAppender esAppender) { this.esAppender = esAppender; }

  @JsonIgnore
  public Map<String, String> getLog4jConfiguration() {
    Map<String, String> props = new LinkedHashMap<String, String>();
    List<String> appenders = new ArrayList<>();
    if(consoleAppender.enable) {
      consoleAppender.addLog4jConfig(props);
      appenders.add("console");
    }
    if(fileAppender.enable) {
      fileAppender.addLog4jConfig(props);
      appenders.add(fileAppender.name) ;
    }
    
    if(esAppender.enable) {
      esAppender.addLog4jConfig(props);
      appenders.add(esAppender.name) ;
    }
    
    if(appenders.size() > 0) {
      props.put("log4j.rootLogger",  logLevel + "," + StringUtil.join(appenders, ","));
    }
    return props ;
  }
  
  public String buildParameters() {
    StringBuilder b = new StringBuilder() ;
    consoleAppender.buildParameters(b);
    fileAppender.buildParameters(b);
    esAppender.buildParameters(b);
    return b.toString() ;
  }

  static public class ConsoleAppender {
    @Parameter(names = "--log-console-enable", description = "Enable or not the console log")
    boolean enable      = false;

    public boolean isEnable() { return enable; }
    public void setEnable(boolean enable) {  this.enable = enable; }

    void addLog4jConfig(Map<String, String> props) {
      props.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender");
      props.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout");
      props.put("log4j.appender.console.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
    }
    
    void buildParameters(StringBuilder b) {
      if(enable) {
        b.append(" --log-console-enable ");
      }
    }
  }

  
  static public class FileAppender {
    @Parameter(names = "--log-file-enable", description = "Enable or not the log file")
    boolean enable      = true;

    @Parameter(names = "--log-file-name", description = "The name of the log file appender")
    String  name        = "file";

    @Parameter(names = "--log-file-path", required = true, description = "The path of the log file")
    String  filePath    = "logs/vm.log";

    @Parameter(names = "--log-file-max-size-in-mb", description = "The maximum size of the log file")
    int     maxSizeInMb = 5;

    @Parameter(names = "--log-file-max-backup", description = "The maximum number of the backup log file")
    int     maxBackup   = 5;

    public boolean isEnable() { return enable; }
    public void setEnable(boolean enable) { this.enable = enable; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }

    public int getMaxSizeInMb() { return maxSizeInMb; }
    public void setMaxSizeInMb(int maxSizeInMb) { this.maxSizeInMb = maxSizeInMb; }

    public int getMaxBackup() { return maxBackup; }
    public void setMaxBackup(int maxBackup) { this.maxBackup = maxBackup; }

    public void addLog4jConfig(Map<String, String> props) {
      String prefix = "log4j.appender." + name;
      props.put(prefix,   "org.apache.log4j.RollingFileAppender");
      props.put(prefix + ".layout", "org.apache.log4j.PatternLayout");
      props.put(prefix + ".layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");
      props.put(prefix + ".File", filePath);
      props.put(prefix + ".MaxFileSize", maxSizeInMb + "MB");
      props.put(prefix + ".MaxBackupIndex", maxBackup + "");
    }
    
    void buildParameters(StringBuilder b) {
      if(!enable) return ;
      b.append(" --log-file-enable ");
      b.append(" --log-file-name ").append(name);
      b.append(" --log-file-path ").append(filePath);
      b.append(" --log-file-max-size-in-mb ").append(maxSizeInMb);
      b.append(" --log-file-max-backup ").append(maxBackup);
    }
  }
  
  static public class ESAppender {
    @Parameter(names = "--log-es-enable", description = "Enable or not the log file")
    boolean        enable    = true;

    @Parameter(names = "--log-es-name", description = "The name of the log es appender")
    private String name      = "es";

    @Parameter(names = "--log-es-connects", description = "The name of the log file appender")
    private String connects  = "elasticsearch-1:9300";

    @Parameter(names = "--log-es-buffer-dir", description = "The buffer directory")
    private String bufferDir = "data/buffer/log4j";

    @Parameter(names = "--log-es-index-name", description = "The name of the log index")
    private String indexName = "log4j";
    
    public boolean isEnable() { return enable; }
    public void setEnable(boolean enable) { this.enable = enable; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getConnects() { return connects; }
    public void setConnects(String connects) { this.connects = connects; }

    public String getBufferDir() { return bufferDir; }
    public void setBufferDir(String bufferDir) { this.bufferDir = bufferDir;}

    public String getIndexName() { return indexName; }
    public void setIndexName(String indexName) { this.indexName = indexName; }

    public void addLog4jConfig(Map<String, String> props) {
      String prefix = "log4j.appender." + name;
      props.put(prefix, "com.neverwinterdp.es.log4j.ElasticSearchAppender");
      props.put(prefix + ".layout", "org.apache.log4j.PatternLayout");
      props.put(prefix + ".layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");

      props.put(prefix + ".connects", connects);
      props.put(prefix + ".indexName", indexName);
      props.put(prefix + ".queueBufferDir", bufferDir);
    }
    
    void buildParameters(StringBuilder b) {
      if(!enable) return ;
      b.append(" --log-es-enable ");
      b.append(" --log-es-name ").append(name);
      b.append(" --log-es-connects ").append(connects);
      b.append(" --log-es-buffer-dir ").append(bufferDir);
      b.append(" --log-es-index-name ").append(indexName);
    }
  }
}