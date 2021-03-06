package net.tuan08.tracking;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.neverwinterdp.util.text.TabularFormater;

public class TrackingMessageReport {
  private String vmId    = "unknown";
  private String chunkId ;
  private int    numOfMessage;
  private int    progress;
  private int    noLostTo;
  private int    lostCount;
  private int    duplicatedCount;
  
  private long   minDeliveryTime;
  private long   maxDeliveryTime;
  private long   avgDeliveryTime;

  public TrackingMessageReport() {} 
  
  public TrackingMessageReport(String vmId, String chunkId, int numOfMessage) {
    this.vmId         = vmId;
    this.chunkId      = chunkId ;
    this.numOfMessage = numOfMessage;
  }
  
  public String getVmId() { return vmId; }
  public void setVmId(String vmId) { this.vmId = vmId; }

  public String getChunkId() { return chunkId; }
  public void   setChunkId(String chunkId) { this.chunkId = chunkId; }
  
  public int getNumOfMessage() { return numOfMessage; }
  public void setNumOfMessage(int numOfMessage) { this.numOfMessage = numOfMessage; }

  public int getProgress() { return progress; }
  public void setProgress(int progress) { this.progress = progress; }

  public int getNoLostTo() { return noLostTo;}
  public void setNoLostTo(int noLostTo) { this.noLostTo = noLostTo;}

  public int getLostCount() { return lostCount; }
  public void setLostCount(int lostCount) { this.lostCount = lostCount; }

  public int getDuplicatedCount() { return duplicatedCount; }
  public void setDuplicatedCount(int duplicatedCount) { this.duplicatedCount = duplicatedCount; }
  
  public long getMinDeliveryTime() { return minDeliveryTime; }
  public void setMinDeliveryTime(long minDeliveryTime) { this.minDeliveryTime = minDeliveryTime; }

  public long getMaxDeliveryTime() { return maxDeliveryTime; }
  public void setMaxDeliveryTime(long maxDeliveryTime) { this.maxDeliveryTime = maxDeliveryTime; }

  public long getAvgDeliveryTime() { return avgDeliveryTime; }
  public void setAvgDeliveryTime(long avgDeliveryTime) { this.avgDeliveryTime = avgDeliveryTime; }

  public String reportName() { return vmId + "." + chunkId; }
  
  @JsonIgnore
  public boolean isComplete() {
    return noLostTo == progress && progress == numOfMessage;
  }
  
  static public String getFormattedReport(String title, List<TrackingMessageReport> reports) {
    String[] header = {
      "VM Id", "Chunk Id", "Num Of Message", "Progress", "No Lost To", "Lost", "Duplicated", 
      "Min D Time", "Max D Time", "Avg D Time"
    };
    TabularFormater formater = new TabularFormater(header);
    formater.setTitle(title);
    for(TrackingMessageReport report : reports) {
      formater.addRow(
        report.getVmId(), report.getChunkId(), report.getNumOfMessage(), report.getProgress(), 
        report.getNoLostTo(), report.getLostCount(), report.getDuplicatedCount(),
        report.getMinDeliveryTime(), report.getMaxDeliveryTime(), report.getAvgDeliveryTime()
      );
    }
    return formater.getFormattedText() ;
  }
}
