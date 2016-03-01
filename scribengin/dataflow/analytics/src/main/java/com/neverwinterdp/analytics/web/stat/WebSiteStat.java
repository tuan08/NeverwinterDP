package com.neverwinterdp.analytics.web.stat;

import java.util.Date;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.neverwinterdp.analytics.web.WebEvent;
import com.neverwinterdp.util.UrlParser;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

public class WebSiteStat {
  @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="dd/MM/yyyy HH:mm:ss")
  private Date   timestamp;
  
  private String host;
  private int    hitCount;
  private HashSet<String> tags = new HashSet<String>();
  
  public WebSiteStat() { }
  
  public WebSiteStat(Date timestamp, UrlParser urlParser) {
    this.timestamp = timestamp;
    this.host      = urlParser.getHost();
  }
  
  public String uniqueId() {
    return host + "#" + DateUtil.asCompactDateTime(timestamp);
  }
  
  public Date getTimestamp() { return timestamp; }
  public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }

  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }

  public int getHitCount() { return hitCount; }
  public void setHitCount(int hitCount) { this.hitCount = hitCount; }

  public String[] getTags() { return tags.toArray(new String[tags.size()]); }

  public void setTags(String[] tags) {
    for(String sel : tags) this.tags.add(sel);
  }
  
  public void addTag(String tag) {
    tags.add(tag);
  }
  
  public void log(long periodTimestamp, UrlParser urlParser, WebEvent webEvent) {
    hitCount++;
    if(urlParser.getHost().startsWith("www.website-")) {
      addTag("source:generator");
    } else {
      addTag("source:user");
    }
  }
  
  static public String getFormattedText(List<WebSiteStat> holder) {
    TabularFormater formatter = new TabularFormater(
      "Host", "Timestamp", "Hit Count"
    ) ;
    formatter.setTitle("Web Page Statistic");
    for(int i = 0; i < holder.size(); i++) {
      WebSiteStat wpStat = holder.get(i);
      String timestamp = DateUtil.asCompactDateTime(wpStat.getTimestamp());
      formatter.addRow(wpStat.getHost(), timestamp, wpStat.getHitCount());
    }
    return formatter.getFormattedText();
  }
}