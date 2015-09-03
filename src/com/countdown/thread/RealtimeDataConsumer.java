/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.thread;

import com.countdown.db.DBManager;
import com.countdown.queue.RealtimeDataQueue;
import com.countdown.thread.meshjoin.StreamPartitionQueue;
import java.util.HashMap;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maochuanli
 */
public class RealtimeDataConsumer extends Thread {

    private volatile boolean running = true;
    private Queue realtimeInputQueue = null;
    private int inputQueueMaxSize = 0;

    public RealtimeDataConsumer() {
        this.setName("Realtime.Data.Extractor.T");
    }

    public synchronized void setStopFlag() {
        running = false;
    }

    /**
     * To get db connection
     */
    public boolean setup() {
        realtimeInputQueue = RealtimeDataQueue.getInputDataQueue();
        return realtimeInputQueue != null;
    }

    public void tearDown() {
        DBManager.closeInConnection(DBManager.IN_MASTER);
    }

    @Override
    public void run() {
        while (running) {
            int qSize = realtimeInputQueue.size();

            if (qSize > 0) {
                HashMap record = (HashMap) realtimeInputQueue.poll();
                processRecord(record);
                if (qSize > inputQueueMaxSize) {
                    inputQueueMaxSize = qSize;
                }
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {
                    Logger.getLogger(RealtimeDataConsumer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        while (realtimeInputQueue.size() > 0) {
            HashMap record = (HashMap) realtimeInputQueue.poll();
            processRecord(record);
        }
        //flush the data in stream partition queue
        StreamPartitionQueue.flushPartitions();
        info("Meshjoin Worker Completed! Max Input Queue Size: " + inputQueueMaxSize);
    }

    private void processRecord(HashMap record) {
        StreamPartitionQueue.addRecord(record);
    }

    void info(String msg) {
        Logger.getLogger(RealtimeDataConsumer.class.getName()).log(Level.INFO, msg);
    }

}
