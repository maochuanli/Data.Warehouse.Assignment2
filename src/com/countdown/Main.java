/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown;

import com.countdown.bean.pool.RecordFactory;
import com.countdown.bean.pool.TransactionBean;
import com.countdown.thread.DataLoaderWorker;
import com.countdown.thread.StreamQueueAssemblyWorker;
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
            
            new Main().testMeshJoin();
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
    
    void test() throws Exception{
        RealtimeDataProducer dataProducer = new RealtimeDataProducer();
        dataProducer.setup();
        StreamQueueAssemblyWorker meshjoiner = new StreamQueueAssemblyWorker();
        meshjoiner.setup();
        
        meshjoiner.start();
        dataProducer.start();
        
        dataProducer.join();
        Thread.sleep(100);
        meshjoiner.setStopFlag();
        
        System.out.println(meshjoiner.toString());
    }
    
    void testMeshJoin(){
        
        DataLoaderWorker loaderT = new DataLoaderWorker();
        loaderT.setup();
        loaderT.start();
        
        for(int i=0;i<51;i++){
            HashMap record;
            try {
                record = getObjectPool().borrowObject();
                record.put(TransactionBean.TRANSACTION_ID, i);
                record.put(TransactionBean.PRODUCT_ID, "P-"+(1000+i)+"");
//            record.put(TransactionBean.CUSTOMER_ID, result.getString(TransactionBean.CUSTOMER_ID));
//            record.put(TransactionBean.CUSTOMER_NAME, result.getString(TransactionBean.CUSTOMER_NAME));
//            record.put(TransactionBean.STORE_ID, result.getString(TransactionBean.STORE_ID));
//            record.put(TransactionBean.STORE_NAME, result.getString(TransactionBean.STORE_NAME));
//            record.put(TransactionBean.TRANSACTION_DATE, result.getDate(TransactionBean.TRANSACTION_DATE));
//            record.put(TransactionBean.TRANSACTION_QUANTITY, result.getLong(TransactionBean.TRANSACTION_QUANTITY));
                if(i%10 ==0){
                    record.put(TransactionBean.PRODUCT_ID, "P-"+(100+i)+"");
                }
                StreamPartitionQueue.addRecord(record);
            } catch (Exception ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        
        try {
            System.out.println("wait for thread to complete...");
            System.in.read();
            loaderT.setStopFlag();
            loaderT.join();
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
