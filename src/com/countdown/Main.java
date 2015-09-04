/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown;

import com.countdown.bean.pool.RecordFactory;
import com.countdown.thread.DataLoaderWorker;
import com.countdown.thread.RealtimeDataConsumer;
import com.countdown.thread.RealtimeDataProducer;

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
    private static Logger LOGGER = Logger.getLogger(Main.class.getName());
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            new Main().executeETL();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

    void executeETL() throws Exception {
        //start the Data Warehouse Loader Thread
        LOGGER.info("Start up the Data Loader Thread...");
        DataLoaderWorker loaderT = new DataLoaderWorker();
        loaderT.setup();
        loaderT.start();

        //Start the Meshjoin Realtime Extractor Thread
        LOGGER.info("Start up the Meshjoin Data Extractor Thread...");
        RealtimeDataConsumer meshjoiner = new RealtimeDataConsumer();
        meshjoiner.setup();
        meshjoiner.start();

        //Start the Realtime Data Producer Thread
        LOGGER.info("Start up the Realtime Data Producer Thread...");
        RealtimeDataProducer dataProducer = new RealtimeDataProducer();
        dataProducer.setup();
        dataProducer.start();

        //Wait for the producer thread to complete
        dataProducer.join();
        dataProducer.tearDown();
        LOGGER.info("Realtime Data Producer Thread Completed!");

        //Wait for the extractor thread to complete
        meshjoiner.setStopFlag();
        meshjoiner.join();
        meshjoiner.tearDown();
        LOGGER.info("Meshjoin Data Extractor Thread Completed!");

        //Wait for the data loader thread to complete
        loaderT.setStopFlag();
        loaderT.join();
        loaderT.tearDown();
        LOGGER.info("Data Loader Thread Completed!");

        LOGGER.severe("ALL DONE! Please execute the OLAP queries on Data Warehouse.");
    }

    public static synchronized ObjectPool<HashMap> getObjectPool() {
        if (pool == null) {
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
            RecordFactory factory = new RecordFactory();
            config.setMaxTotal(20000);
            pool = new GenericObjectPool<>(factory, config);
        }
        return pool;
    }
}
