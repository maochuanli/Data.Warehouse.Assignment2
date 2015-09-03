/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown;

import com.countdown.thread.meshjoin.StreamPartitionQueue;
import com.countdown.bean.pool.RecordFactory;
import com.countdown.bean.pool.TransactionBean;
import com.countdown.thread.DataLoaderWorker;
import com.countdown.thread.RealtimeDataConsumer;
import com.countdown.thread.RealtimeDataProducer;
import java.io.IOException;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 *
 * @author maochuanli
 */
public class Main {

    private static ObjectPool<HashMap> pool;
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            
            new Main().execute();
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static synchronized ObjectPool<HashMap> getObjectPool(){
        if(pool==null){
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
            RecordFactory factory = new RecordFactory();
            config.setMaxTotal(20000);
            pool = new GenericObjectPool<>(factory, config);
        }
        return pool;
    }
    
    void execute() throws Exception{
        DataLoaderWorker loaderT = new DataLoaderWorker();
        loaderT.setup();
        loaderT.start();
        
        RealtimeDataProducer dataProducer = new RealtimeDataProducer();
        dataProducer.setup();
        
        RealtimeDataConsumer meshjoiner = new RealtimeDataConsumer();
        meshjoiner.setup();        
        meshjoiner.start();
        
        dataProducer.start();
        
        dataProducer.join();
        dataProducer.tearDown();
        
        meshjoiner.setStopFlag();
        
        meshjoiner.join();
        meshjoiner.tearDown();
        
        
        loaderT.setStopFlag();
        loaderT.join();
        loaderT.tearDown();
        
        System.out.println("DONE ETL!");
    }
    
}
