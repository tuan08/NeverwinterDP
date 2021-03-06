package com.neverwinterdp.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

public class S3ObjectWriter {
  private S3Client s3Client;
  private String bucketName;
  private String key;
  private ObjectMetadata metadata ;
  private ByteArrayOutputStream bos ;
  private ObjectOutputStream    objOs;
  private int numOfRecords = 0;
  
  public S3ObjectWriter(S3Client s3Client, String bucketName, String key, ObjectMetadata metadata) throws IOException {
    this.s3Client   = s3Client;
    this.bucketName = bucketName;
    this.key        = key;
    this.metadata   = metadata;
    this.bos        = new ByteArrayOutputStream(4 * 1024 * 1024) ;
    this.objOs      = new ObjectOutputStream(bos);
  }

  public ObjectMetadata getObjectMetadata() { return metadata; }

  public void write(byte[] data) throws IOException {
    objOs.writeInt(data.length);
    objOs.write(data);
    numOfRecords++;
  }

  public void waitAndClose(long timeout) throws Exception, IOException, InterruptedException {
    objOs.flush();
    bos.flush();
    byte[] data = bos.toByteArray();
    if(data.length == 0) return;
    Throwable error = null;
    for(int i = 0; i < 3; i++) {
      try {
        tryWaitAndClose(data, timeout);
        return;
      } catch (AmazonServiceException ase) {
        System.err.println("waitAndClose: try = "  + i);
        System.err.println("Error Message:    " + ase.getMessage());
        System.err.println("HTTP Status Code: " + ase.getStatusCode());
        System.err.println("AWS Error Code:   " + ase.getErrorCode());
        System.err.println("Error Type:       " + ase.getErrorType());
        System.err.println("Request ID:       " + ase.getRequestId());
        error = ase;
        continue ;
      } catch(Throwable t) {
        error = t ;
        break;
      }
    }
    error.printStackTrace(System.err);
    throw new IOException(error);
  }
  
  public void forceClose() throws IOException, InterruptedException {
    objOs.close();
  }
  
  private void tryWaitAndClose(byte[] data, long timeout) throws AmazonServiceException, AmazonClientException, InterruptedException {
    ByteArrayInputStream input = new ByteArrayInputStream(data);
    metadata.setContentLength(data.length);
    metadata.addUserMetadata("num-of-records", Integer.toString(numOfRecords));
    PutObjectRequest request = new PutObjectRequest(bucketName, key, input, metadata);
    //request.getRequestClientOptions().setReadLimit(256 * 1024);
    long start = System.currentTimeMillis();
    UploadProgressListener uploadListener = new UploadProgressListener();
    request.setGeneralProgressListener(uploadListener);
    PutObjectResult result = s3Client.getAmazonS3Client().putObject(request);
    uploadListener.waitForUploadComplete(timeout);
    if(uploadListener.getComleteProgressEvent() == null) {
      String mesg = 
          "Cannot get the complete event after " + timeout + "ms\n" + 
          uploadListener.getProgressEventInfo();
      System.err.println(mesg);
      throw new AmazonServiceException(mesg);
    }
  }
  
  static public class UploadProgressListener implements ProgressListener {
    private int requestByteTransferEvent = 0;
    private ProgressEvent lastRequestByteTransferEvent  ;
    private StringBuilder progressEvents = new StringBuilder();
    private ProgressEvent completeEvent ;
    
    @Override
    synchronized public void progressChanged(ProgressEvent progressEvent) {
      if(progressEvent.getEventType() == ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT) {
        requestByteTransferEvent++;
        lastRequestByteTransferEvent = progressEvent;
      } else {
        progressEvents.append(progressEvent).append("\n");
      }
      
      if(progressEvent.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
        completeEvent = progressEvent;
        notifyComplete();
      } else if(progressEvent.getEventType() == ProgressEventType.TRANSFER_FAILED_EVENT) {
        System.err.println("Exception: Got Failed Event....");
      }
    }
    
    public ProgressEvent getComleteProgressEvent() { return  completeEvent; }
    
    public String getProgressEventInfo() {
      String info = 
          "lastRequestByteTransferEvent = " + lastRequestByteTransferEvent + "\n" +
          "REQUEST_BYTE_TRANSFER_EVENT = " + requestByteTransferEvent + "\n" +
          progressEvents.toString();
      return info;
    }
    
    synchronized void notifyComplete() {
       notifyAll();
    }
    
    synchronized void waitForUploadComplete(long timeout) throws InterruptedException {
      if(completeEvent != null) return;
      wait(timeout);
   }
  }
}
