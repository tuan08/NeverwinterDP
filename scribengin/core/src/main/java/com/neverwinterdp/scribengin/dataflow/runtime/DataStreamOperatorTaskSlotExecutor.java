package com.neverwinterdp.scribengin.dataflow.runtime;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.task.TaskStatus;
import com.neverwinterdp.registry.task.dedicated.DedicatedTaskContext;
import com.neverwinterdp.registry.task.dedicated.TaskSlotExecutor;
import com.neverwinterdp.scribengin.dataflow.api.DataStreamOperator;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.worker.WorkerService;
import com.neverwinterdp.storage.Record;

public class DataStreamOperatorTaskSlotExecutor extends TaskSlotExecutor<DataStreamOperatorDescriptor>{
  private WorkerService                            workerService;
  private DedicatedTaskContext<DataStreamOperatorDescriptor> taskContext;
  private DataStreamOperatorDescriptor                       operatorTaskConfig;
  private DataStreamOperator                                 operator;
  private DataStreamOperatorRuntimeContext                          context;
  
  private long                                     startTime = 0;
  private long                                     lastFlushTime = System.currentTimeMillis();
  private long                                     lastNoMessageTime = lastFlushTime;
  
  public DataStreamOperatorTaskSlotExecutor(WorkerService service, DedicatedTaskContext<DataStreamOperatorDescriptor> taskContext) throws Exception {
    super(taskContext);
    this.workerService = service;
    this.taskContext     = taskContext;
    this.operatorTaskConfig      = taskContext.getTaskDescriptor(false);
    Class<DataStreamOperator> opType = (Class<DataStreamOperator>) Class.forName(operatorTaskConfig.getOperator());
    operator = opType.newInstance();
    
    startTime = System.currentTimeMillis();
    DataflowRegistry dRegistry = workerService.getDataflowRegistry();
    DataStreamOperatorReport report = dRegistry.getTaskRegistry().getTaskReport(operatorTaskConfig);
    report.incrAssignedCount();
    dRegistry.getTaskRegistry().save(operatorTaskConfig, report);
    context = new DataStreamOperatorRuntimeContext(workerService, operatorTaskConfig, report);
    dRegistry.getTaskRegistry().save(operatorTaskConfig, report);
  }
  
  public DedicatedTaskContext<DataStreamOperatorDescriptor> getTaskContext() { return this.taskContext; }
  
  public DataStreamOperatorDescriptor getDescriptor() { return operatorTaskConfig ; }
  
  public boolean isComplete() { return context.isComplete() ; }
  
  @Override
  public void onShutdown() throws Exception {
    context.commit();
    context.close();
  }
  
  @Override
  public void executeSlot() throws Exception {
    startTime = System.currentTimeMillis();
    DataStreamOperatorReport report = context.getReport();
    int recCount = 0;
    try {
      while(!isInterrupted()) {
        Record record = context.nextRecord(1000);
        if(record == null) break ;

        recCount++;
        report.incrProcessCount();
        operator.process(context, record);
      } //end while
      
      long currentTime = System.currentTimeMillis();
      if(recCount == 0) {
        if(lastNoMessageTime < 0) lastNoMessageTime = currentTime;
        report.setAssignedWithNoMessageProcess(report.getAssignedWithNoMessageProcess() + 1);
        report.setLastAssignedWithNoMessageProcess(report.getLastAssignedWithNoMessageProcess() + 1);
        if(lastNoMessageTime + 180000 < currentTime) {
          getTaskContext().setComplete();
          context.setComplete();
        }
      } else {
        report.setLastAssignedWithNoMessageProcess(0);
        lastNoMessageTime = -1;
      }
      report.addAccRuntime(currentTime - startTime);
      
      if(context.isComplete() || report.getProcessCount() > 10000 || lastFlushTime + 30000 < currentTime) {
        context.commit();
      }
    } catch(InterruptedException ex) {
      throw ex ;
    } catch(RegistryException error) {
      throw error;
    } catch(Exception error) {
      System.err.println("Catched a task exception");
      error.printStackTrace();
      System.err.println("Rollback");
      rollback(error);
    } catch(Throwable t) {
      System.err.println("Catch Throwable");
      t.printStackTrace();
      throw t;
    }
  }
  
  void rollback(Exception error) throws Exception {
    context.rollback();
    DataStreamOperatorReport report = context.getReport();
    report.setAssignedHasErrorCount(report.getAssignedHasErrorCount() + 1);
    workerService.getLogger().error("DataflowTask Error", error);
  }
  
  public void finish() throws Exception {
    DataStreamOperatorReport report = context.getReport();
    report.setFinishTime(System.currentTimeMillis());
    context.commit();
    context.close();
    DataflowRegistry dflRegistry = workerService.getDataflowRegistry();
    dflRegistry.getTaskRegistry().finish(taskContext, TaskStatus.TERMINATED);
  }
}