package com.neverwinterdp.scribengin.dataflow;

import com.neverwinterdp.storage.Record;

public abstract class DataStreamOperator {
  abstract public void process(DataStreamOperatorContext ctx, Record record) throws Exception;
}