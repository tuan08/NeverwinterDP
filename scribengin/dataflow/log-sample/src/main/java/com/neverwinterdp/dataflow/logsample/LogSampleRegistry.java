package com.neverwinterdp.dataflow.logsample;

import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.ExceptionUtil;

public class LogSampleRegistry {
  final static public String APP_PATH = "/apps/log-sample" ;

  private Registry registry ;
  private Node appNode ;
  private Node generateReportsNode ;
  private Node generateErrorsNode ;
  private Node validateReportsNode ;
  
  public LogSampleRegistry(Registry registry, boolean initRegistry) throws RegistryException {
    this.registry = registry;
    initRegistry(initRegistry) ;
  }
  
  void initRegistry(boolean create) throws RegistryException {
    if(create) {
      appNode = registry.createIfNotExist(APP_PATH);
      generateReportsNode = appNode.createDescendantIfNotExists("generate/reports");
      generateErrorsNode  = appNode.createDescendantIfNotExists("generate/errors");
      validateReportsNode = appNode.createDescendantIfNotExists("validate/reports");
    } else {
      appNode = registry.get(APP_PATH);
      generateReportsNode = appNode.getDescendant("generate/reports");
      generateErrorsNode  = appNode.getDescendant("generate/errors");
      validateReportsNode = appNode.getDescendant("validate/reports");
    }
  }

  public void addGenerateReport(LogMessageReport report) throws RegistryException {
    generateReportsNode.createChild(report.getGroupId(), report, NodeCreateMode.PERSISTENT);
  }
  
  public List<LogMessageReport> getGeneratedReports() throws RegistryException {
    return generateReportsNode.getChildrenAs(LogMessageReport.class) ;
  }
  
  public void addGenerateError(String groupId, Throwable error) throws RegistryException {
    String stacktrace = ExceptionUtil.getStackTrace(error);
    generateReportsNode.createChild(groupId, stacktrace, NodeCreateMode.PERSISTENT);
  }
  
  public void addValidateReport(LogMessageReport report) throws RegistryException {
    validateReportsNode.createChild(report.getGroupId(), report, NodeCreateMode.PERSISTENT);
  }
  
  public List<LogMessageReport> getValidateReports() throws RegistryException {
    return validateReportsNode.getChildrenAs(LogMessageReport.class) ;
  }
}
