package com.neverwinterdp.netty.http;

import java.util.Map;

import org.slf4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public interface RouteHandler {
  public void setLogger(Logger logger) ;
  public void configure(Map<String, String> props) ;
  public void handle(ChannelHandlerContext ctx, HttpRequest request) ;
  public void close() ;
}
