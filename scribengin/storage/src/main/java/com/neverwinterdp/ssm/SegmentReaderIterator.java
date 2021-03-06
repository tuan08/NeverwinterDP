package com.neverwinterdp.ssm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.ssm.SegmentReader.DataAvailability;

public class SegmentReaderIterator {
  private List<SegmentReader> allSegmentReaders = new ArrayList<>();
  private List<SegmentReader> activeSegmentReaders = new ArrayList<>();
  private int                 lastReadSegmentIdx = 0;
  private int                 lastAddedSegmentId = -1;
  private SegmentReader       currentReader;
  
  public void add(SegmentReader segmentReader) throws IOException, RegistryException {
    int segId     = segmentReader.getSegmentDescriptor().getId();
    if(lastAddedSegmentId >= 0 && lastAddedSegmentId + 1 != segId) {
      throw new IOException("The segment is not in sequence. previous id =  " + lastAddedSegmentId + ", id = " + segId);
    }
    
    allSegmentReaders.add(segmentReader);
    lastAddedSegmentId = segId;
    
    DataAvailability dataAvailability = segmentReader.getDataAvailability();
    if(dataAvailability != DataAvailability.EOS) {
      activeSegmentReaders.add(segmentReader);
    }
  }
  
  public byte[] nextRecord() throws IOException, RegistryException {
    if(currentReader != null && currentReader.hasAvailableData()) {
      return currentReader.nextRecord();
    }
    byte[] record = findNextRecord();
    if(record != null) return record;
    refresh();
    return findNextRecord();
  }
  
  public byte[] findNextRecord() throws IOException, RegistryException {
    int availableReaders = activeSegmentReaders.size();
    if(availableReaders == 0) return null;
    if(lastReadSegmentIdx >= availableReaders) lastReadSegmentIdx = 0;
    while(lastReadSegmentIdx < availableReaders) {
      currentReader = activeSegmentReaders.get(lastReadSegmentIdx);
      if(currentReader.hasAvailableData()) {
        return currentReader.nextRecord();
      }
      lastReadSegmentIdx++;
    }
    return null;
  }
  
  void refresh() throws RegistryException, IOException {
    Iterator<SegmentReader>  i = activeSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader segReader = i.next();
      DataAvailability dataAvailability = segReader.updateAndGetSegmentDescriptor(); 
      if(dataAvailability == DataAvailability.EOS) {
        i.remove();
      }
    }
  }
  
  public void prepareCommit(Transaction transaction) throws IOException, RegistryException {
    Iterator<SegmentReader> i = allSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader reader = i.next();
      reader.prepareCommit(transaction);
    }
  }
  
  public void completeCommit(Transaction transaction) throws IOException, RegistryException {
    Iterator<SegmentReader> i = allSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader reader = i.next();
      reader.completeCommit(transaction);
      if(reader.isComplete()) {
        reader.close();
        i.remove();
      }
    }
  }
  
  public void rollback(Transaction transaction) throws IOException, RegistryException {
    activeSegmentReaders.clear();
    Iterator<SegmentReader> i = allSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader reader = i.next();
      reader.rollback(transaction);
      DataAvailability availability  = reader.getDataAvailability() ;
      if(availability == DataAvailability.YES || availability == DataAvailability.YES) {
        activeSegmentReaders.add(reader);
      }
    }
  }
  
  public void close() throws IOException, RegistryException {
    Iterator<SegmentReader> i = allSegmentReaders.iterator();
    while(i.hasNext()) {
      SegmentReader reader = i.next();
      reader.close();
    }
    activeSegmentReaders.clear();
    allSegmentReaders.clear();
  }
}