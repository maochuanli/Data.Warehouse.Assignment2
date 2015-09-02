/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown;

import com.countdown.bean.pool.TransactionBean;
import com.countdown.thread.StreamQueueAssemblyWorker;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maochuanli
 */
public class StreamPartitionQueue {
    public static final int partitions = MainSystem.getMeshjoinPartitions();
    private final ConcurrentLinkedQueue queue = new ConcurrentLinkedQueue();
    
    private long partitionID = 0;
    private StreamPartition currentPartition;
    private static final StreamPartitionQueue INSTANCE = new StreamPartitionQueue();
    private StreamPartitionQueue(){}
    
    
    public static synchronized void flushPartitions(){
        INSTANCE.info("Flush all the left records in last partition!");
        
        if(INSTANCE.currentPartition != null){
            INSTANCE.queue.offer(INSTANCE.currentPartition);
            MeshjoinWorker.processMeshJoin(INSTANCE.currentPartition);  
            INSTANCE.currentPartition = null;
        }
        
        for(int i=0; i<partitions;i++){
            INSTANCE.partitionID++;
            StreamPartition dummyPartition = new StreamPartition(INSTANCE.partitionID);
            INSTANCE.info("StreamPartitionQueue. new dummy partition id: "+ INSTANCE.partitionID);
            
            INSTANCE.queue.offer(dummyPartition);
            MeshjoinWorker.processMeshJoin(dummyPartition);
        }
    }
    /**
     * Add one record to
     * @param record 
     */
    public static synchronized void addRecord(HashMap record){
//        Long id = (Long) record.get(TransactionBean.TRANSACTION_ID);
//        if(id==50){
//            INSTANCE.info("StreamPartitionQueue: got 50");
//        }
        
        StreamPartition partition = INSTANCE.getCurrentStreamPartition();
        
        partition.addRecord(record);
        
        if(INSTANCE.currentPartition.isFull()){
            INSTANCE.info("partition full, push it into queue, process it");
            INSTANCE.queue.offer(INSTANCE.currentPartition);
            MeshjoinWorker.processMeshJoin(INSTANCE.currentPartition);
            INSTANCE.info("SteamPartitionQueue.current partition size: is full: "+INSTANCE.currentPartition.getPartitionTupleList().size());
            INSTANCE.currentPartition = null;
        }
    }
    
    public static synchronized StreamPartition pollPartition(){
        if(INSTANCE.queue.size() > 0){
//            StreamPartition topPartition = (StreamPartition) INSTANCE.queue.peek();
//            if(INSTANCE.partitionID - topPartition.getPartitionID() >= INSTANCE.partitions){
                return (StreamPartition) INSTANCE.queue.poll();
//            }
        }
        return null;
    }
    
    private StreamPartition getCurrentStreamPartition() {
        if (currentPartition == null) {
            partitionID++;
            currentPartition = new StreamPartition(partitionID);
            info("StreamPartitionQueue. new partition id: "+ currentPartition.getPartitionID());
        }
        
        return currentPartition;
        
    }
    
    void info(String msg){
        Logger.getLogger(StreamPartitionQueue.class.getName()).log(Level.INFO, msg);
    }
}
