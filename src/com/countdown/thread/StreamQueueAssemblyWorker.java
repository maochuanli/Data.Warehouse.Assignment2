/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.thread;

import com.countdown.StreamPartitionQueue;
import java.util.HashMap;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maochuanli
 */
public class StreamQueueAssemblyWorker extends Thread{
    private volatile boolean running = true;
    private Queue realtimeQueue = null;
    
    public synchronized void setStopFlag(){
        running = false;
    }
    /**
     * To get db connection
     */
    public boolean setup(){
        realtimeQueue = RealtimeDataQueue.getInputDataQueue();
     
        return realtimeQueue != null;
    }
    @Override
    public void run() {
        while(running){
            
            if(realtimeQueue.size()>0){
                HashMap record = (HashMap) realtimeQueue.poll();
                processRecord(record);
            }else{
                try {
                    System.out.print(".");
                    Thread.sleep(5);
                    
                } catch (InterruptedException ex) {
                    Logger.getLogger(StreamQueueAssemblyWorker.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        while(realtimeQueue.size()>0){
            HashMap record = (HashMap) realtimeQueue.poll();
            processRecord(record);
        }
        
        System.out.println("Meshjoin Worker Completed!");
    }

    private void processRecord(HashMap record) {
        StreamPartitionQueue.addRecord(record);
    }
    
    public String toString(){
        return "queue size: "+this.realtimeQueue.size();
    }


}
