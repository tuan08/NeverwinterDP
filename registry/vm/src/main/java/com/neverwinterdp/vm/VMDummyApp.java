package com.neverwinterdp.vm;

public class VMDummyApp extends VMApp {
  private String name ;

  @Override
  public void run() throws Exception {
    name = getVM().getDescriptor().getVmConfig().getProperties().get("name") ;
    try {
      while(true) {
        System.out.println("Hello " + name);
        Thread.sleep(1000);
      }
    } catch(InterruptedException ex) {

    }
  }
}