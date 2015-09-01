/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown;

import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    
    /**
     * Add one record to
     * @param record 
     */
    public static synchronized void addRecord(HashMap record){
        StreamPartition partition = INSTANCE.getStreamPartition();
        
        partition.addRecord(record);
        
        if(INSTANCE.currentPartition.isFull()){
            System.out.println("partition full, push it into queue, process it");
            INSTANCE.queue.offer(INSTANCE.currentPartition);
            MeshjoinWorker.processMeshJoin(INSTANCE.currentPartition);
            INSTANCE.currentPartition = null;
        }
    }
    
    public static synchronized StreamPartition pollPartition(){
        if(INSTANCE.queue.size() > 0){
            StreamPartition topPartition = (StreamPartition) INSTANCE.queue.peek();
            if(INSTANCE.partitionID - topPartition.getPartitionID() >= INSTANCE.partitions){
                return (StreamPartition) INSTANCE.queue.poll();
            }
        }
        return null;
    }
    
    private StreamPartition getStreamPartition() {
        if (currentPartition == null) {
            currentPartition = new StreamPartition(++partitionID);
        }
        
        return currentPartition;
        
    }
}
