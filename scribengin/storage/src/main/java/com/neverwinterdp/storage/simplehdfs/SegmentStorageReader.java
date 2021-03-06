package com.neverwinterdp.storage.simplehdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.storage.source.CommitPoint;
import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 */
public class SegmentStorageReader<T> {
  private String            name;
  private FileSystem        fs;
  private String            location;
  private Class<T>          type;
  private List<Path>        dataPaths          = new ArrayList<Path>();
  private int               currentDataPathPos = -1;
  private FSDataInputStream currentDataPathInputStream;
  private boolean           endOfStream        = false;

  private int commitPoint;
  private int currPosition;
  private CommitPoint lastCommitInfo;

  public SegmentStorageReader(String name, FileSystem fs, String location, Class<T> type) throws FileNotFoundException, IllegalArgumentException, IOException {
    this.name = name;
    this.fs = fs;
    this.location = location;
    this.type = type;
    FileStatus[] status = fs.listStatus(new Path(location));
    for (int i = 0; i < status.length; i++) {
      String path = status[i].getPath().getName();
      if(path.startsWith("segment-") && path.endsWith(".data")) {
        dataPaths.add(status[i].getPath());
      }
    }
  }

  public String getName() { return name; }

  public T next(long maxWait) throws IOException {
    if(currentDataPathInputStream == null) {
      currentDataPathInputStream = nextDataPathInputStream();
      if (currentDataPathInputStream == null) return null;
    }
    
    if(currentDataPathInputStream.available() <= 0) {
      currentDataPathInputStream.close();
      currentDataPathInputStream = nextDataPathInputStream();
      if (currentDataPathInputStream == null) return null;
    }
    
    int recordSize = currentDataPathInputStream.readInt();
    byte[] data = new byte[recordSize];
    currentDataPathInputStream.readFully(data);
    return JSONSerializer.INSTANCE.fromBytes(data, type);
  }

  public Message[] next(int size, long maxWait) throws IOException {
    List<T> holder = new ArrayList<>();
    Message[] array = new Message[holder.size()];
    for (int i = 0; i < size; i++) {
      T record = next(maxWait);
      if (record != null)
        holder.add(record);
      else
        break;
    }
    holder.toArray(array);
    return array;
  }

  public boolean isEndOfDataStream() { return endOfStream; }
  
  public void rollback() throws Exception {
    System.err.println("This method is not implemented");
    currPosition = commitPoint;
  }

  public void prepareCommit() {
  }

  public void completeCommit() {
    // TODO Auto-generated method stub
  }

  public void commit() throws IOException {
    System.err.println("This method is not implemented");
    lastCommitInfo = new CommitPoint(name, commitPoint, currPosition);
    this.commitPoint = currPosition;
  }

  public CommitPoint getLastCommitInfo() {
    return this.lastCommitInfo;
  }

  public void close() throws IOException {
    if(currentDataPathInputStream != null) {
      currentDataPathInputStream.close();
    }
  }

  private FSDataInputStream nextDataPathInputStream() throws IOException {
    currentDataPathPos++;
    if (currentDataPathPos >= dataPaths.size()) {
      endOfStream = true;
      return null;
    }
    FSDataInputStream is = fs.open(dataPaths.get(currentDataPathPos));
    if(is.available() <= 0) {
      is.close();
      return nextDataPathInputStream();
    }
    return is;
  }
}