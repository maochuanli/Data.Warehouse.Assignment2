/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.thread;

import com.countdown.queue.RealtimeDataQueue;
import com.countdown.Main;
import com.countdown.bean.pool.TransactionBean;
import com.countdown.db.DBManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maochuanli
 */
public class RealtimeDataProducer extends Thread {

    private Long lastProcessedTID = (long) -1;
    private Connection inConnection;
    private volatile boolean running = true;
    private Queue realtimeInputQueue = null;
    private final String SQL = "select * from KQC3001.TRANSACTIONS  order by transaction_id"; //where transaction_id<51

    public RealtimeDataProducer() {
        this.setName("Realtime.Data.Producer.T");
        this.setPriority(MIN_PRIORITY);
    }

    public synchronized void setStopFlag() {
        running = false;
    }

    /**
     * To get db connection
     */
    public boolean setup() {
        realtimeInputQueue = RealtimeDataQueue.getInputDataQueue();

        inConnection = DBManager.getInConnection(DBManager.IN_TRANS);
        if (inConnection != null) {
            try {
                return inConnection.isValid(0);
            } catch (SQLException ex) {
                Logger.getLogger(RealtimeDataProducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return false;
    }

    public void tearDown() {
        DBManager.closeInConnection(DBManager.IN_TRANS);
    }

    @Override
    public void run() {

        try {
            Statement statement = inConnection.createStatement();
            ResultSet result = statement.executeQuery(SQL);

            while (running && result.next()) {
                processRecord(result);
            }

            result.close();
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(RealtimeDataProducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void processRecord(ResultSet result) {
        try {
            HashMap record = Main.getObjectPool().borrowObject();
            lastProcessedTID = result.getLong(TransactionBean.TRANSACTION_ID);

            record.put(TransactionBean.TRANSACTION_ID, lastProcessedTID);
            record.put(TransactionBean.PRODUCT_ID, result.getString(TransactionBean.PRODUCT_ID));
            record.put(TransactionBean.CUSTOMER_ID, result.getString(TransactionBean.CUSTOMER_ID));
            record.put(TransactionBean.CUSTOMER_NAME, result.getString(TransactionBean.CUSTOMER_NAME));
            record.put(TransactionBean.STORE_ID, result.getString(TransactionBean.STORE_ID));
            record.put(TransactionBean.STORE_NAME, result.getString(TransactionBean.STORE_NAME));
            record.put(TransactionBean.TRANSACTION_DATE, result.getDate(TransactionBean.TRANSACTION_DATE));
            record.put(TransactionBean.TRANSACTION_QUANTITY, result.getLong(TransactionBean.TRANSACTION_QUANTITY));

            realtimeInputQueue.offer(record);

        } catch (Exception ex) {
            Logger.getLogger(RealtimeDataProducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    void info(String msg) {
        Logger.getLogger(RealtimeDataProducer.class.getName()).log(Level.INFO, msg);
    }

}
