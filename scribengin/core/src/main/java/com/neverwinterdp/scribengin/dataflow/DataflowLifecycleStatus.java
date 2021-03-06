package com.neverwinterdp.scribengin.dataflow;

public enum DataflowLifecycleStatus { 
  CREATE((byte)0), INIT((byte)1),   RUNNING((byte)2), 
  PAUSE((byte)3),  STOP((byte)4), TERMINATED((byte)5) ;
  
  private byte level ;
  
  private DataflowLifecycleStatus(byte level) {
    this.level = level ;
  }
  
  public int compare(DataflowLifecycleStatus other) {
    return level - other.level;
  }
  
  public boolean greaterThan(DataflowLifecycleStatus other) {
    return level > other.level ;
  }
  
  public boolean equalOrGreaterThan(DataflowLifecycleStatus other) {
    return level >= other.level ;
  }
}