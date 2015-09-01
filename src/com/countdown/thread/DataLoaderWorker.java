/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.thread;

import java.util.HashMap;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maochuanli
 */
public class DataLoaderWorker extends Thread{

    private volatile boolean running = true;
    private Queue realtimeOutputQueue = null;
    
    public boolean setup(){
        realtimeOutputQueue = RealtimeDataQueue.getOutputDataQueue();
        return true;
    }
    
    public DataLoaderWorker(){
        this.setName("DW.Loader.T");
    }
    
    public synchronized void setStopFlag(){
        running = false;
    }
    
    @Override
    public void run() {
        while(running){
            
            if(realtimeOutputQueue.size()>0){
                HashMap record = (HashMap) realtimeOutputQueue.poll();
                processRecord(record);
            }else{
                try {
//                    System.out.print(".");
                    Thread.sleep(5);
                    
                } catch (InterruptedException ex) {
                    Logger.getLogger(StreamQueueAssemblyWorker.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        while(realtimeOutputQueue.size()>0){
            HashMap record = (HashMap) realtimeOutputQueue.poll();
            processRecord(record);
        }
        
        System.out.println("Data Loader Worker Completed!");
    }

    private void processRecord(HashMap record) {
        
        StringBuilder b = new StringBuilder();
        for(Object key: record.keySet()){
            b.append(key+"="+record.get(key)+",");
        }
        System.out.println("Record: "+ b.toString());
    }
    
}
