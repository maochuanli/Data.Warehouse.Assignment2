/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.thread;

import com.countdown.MainSystem;
import com.countdown.bean.pool.MasterBean;
import com.countdown.queue.RealtimeDataQueue;
import com.countdown.bean.pool.TransactionBean;
import com.countdown.db.DBManager;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maochuanli
 */
public class DataLoaderWorker extends Thread {

    private volatile boolean running = true;
    private Queue realtimeOutputQueue = null;
    private Connection outConn = null;
    private final HashSet productIDSet = new HashSet();
    private final HashSet storeIDSet = new HashSet();
    private final HashSet customerIDSet = new HashSet();
    private final HashSet supplierIDSet = new HashSet();
    private final HashSet dateIDSet = new HashSet();
    private int outputQueueMaxSize = 0;
    private final int batchStmtCount = MainSystem.getInsertBatchSize();
    private int currentBatchCount = 0;
    
    private final String queryTransSQL = "select * from SALES where TRANSACTION_ID=?";
    private final String updateSQL = "INSERT INTO SALES"
                + "(TRANSACTION_ID, PRODUCT_ID, SUPPLIER_ID, CUSTOMER_ID, STORE_ID, DATE_ID, TOTAL_SALE) VALUES"
                + "(?,?,?,?,?,?,?)";
    
    private PreparedStatement transInsertStmt;
    private PreparedStatement queryTransStmt;
    //for Supplier Table
    private final String querySupplierSQL = "select * from SUPPLIERS where SUPPLIER_ID=?";
    private final String updateSupplierSQL = "insert into SUPPLIERS(SUPPLIER_ID,SUPPLIER_NAME) values(?,?)";
    private PreparedStatement querySupplierStmt;
    private PreparedStatement preparedSupplierStmt;
    //for customer table
    private final String queryCustomerSQL = "select * from CUSTOMERS where CUSTOMER_ID=?";
    private final String updateCustomerSQL = "insert into CUSTOMERS values( ?,?)";
    private PreparedStatement queryCustomerStmt;
    private PreparedStatement preparedCustomerStmt;
    //for store table
    private final String queryStoreSQL = "select * from STORES where STORE_ID=?";
    private final String updateStoreSQL = "insert into stores values( ?,? )";
    private PreparedStatement queryStoreStmt;
    private PreparedStatement preparedStoreStmt;
    //product table
    private final String queryProductSQL = "select * from PRODUCTS where PRODUCT_ID=?";
    private final String updateProductSQL = "insert into PRODUCTS values(?,?)";
    private PreparedStatement queryProductStmt;
    private PreparedStatement preparedProductStmt;
    //dates table
    private final String queryDateSQL = "select * from DATES where DATE_ID=?";
    private final String updateDateSQL = "INSERT INTO DATES"
                + "(DATE_ID, DD, MM, QTR, YYYY, WEEK) VALUES"
                + "(?,?,?,?,?,?)";
    private Calendar calendar = new GregorianCalendar();
    private PreparedStatement queryDateStmt;
    private PreparedStatement preparedDateStmt;
        
    public DataLoaderWorker() {
        this.setName("DW.DataLoader.T");
        this.setPriority(Thread.MAX_PRIORITY);
    }

    public boolean setup() {
        realtimeOutputQueue = RealtimeDataQueue.getOutputDataQueue();
        outConn = DBManager.getOutConnection();
        if (outConn != null) {
            try {
                //transactions/sales table
                this.queryTransStmt = outConn.prepareStatement(queryTransSQL);
                this.transInsertStmt = outConn.prepareStatement(updateSQL);
                //supplier
                querySupplierStmt = outConn.prepareStatement(querySupplierSQL);
                preparedSupplierStmt = outConn.prepareStatement(updateSupplierSQL);
                //customer                 
                queryCustomerStmt = outConn.prepareStatement(queryCustomerSQL);
                preparedCustomerStmt = outConn.prepareStatement(updateCustomerSQL);
                //store                 
                queryStoreStmt = outConn.prepareStatement(queryStoreSQL);
                preparedStoreStmt = outConn.prepareStatement(updateStoreSQL);
                //product
                queryProductStmt = outConn.prepareStatement(queryProductSQL);
                preparedProductStmt = outConn.prepareStatement(updateProductSQL);
                //dates
                queryDateStmt = outConn.prepareStatement(queryDateSQL);
                preparedDateStmt = outConn.prepareStatement(updateDateSQL);
                
                return outConn.isValid(0);
            } catch (SQLException ex) {
                Logger.getLogger(RealtimeDataProducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }        
        return false;
    }

    public void tearDown() {
        try {
            //sales
            this.queryTransStmt.close();
            this.transInsertStmt.close();
            //supplier
            this.querySupplierStmt.close();
            this.preparedSupplierStmt.close();
            //customer
            this.queryCustomerStmt.close();
            this.preparedCustomerStmt.close();
            //store
            this.queryStoreStmt.close();
            this.preparedStoreStmt.close();
            //product
            this.queryProductStmt.close();
            this.preparedProductStmt.close();
            //dates
            this.queryDateStmt.close();
            this.preparedDateStmt.close();
            
        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        DBManager.closeOutConnection();
    }

    public void setStopFlag() {
        running = false;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        while (running) {
            int qSize = realtimeOutputQueue.size();
            if (qSize > 0) {
                HashMap record = (HashMap) realtimeOutputQueue.poll();
                processRecord(record);
                if(qSize > outputQueueMaxSize){
                    outputQueueMaxSize = qSize;
                }
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {
                    Logger.getLogger(RealtimeDataConsumer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        while (realtimeOutputQueue.size() > 0) {
            HashMap record = (HashMap) realtimeOutputQueue.poll();
            processRecord(record);
        }
        commitTransBatch();
        long end = System.currentTimeMillis();
        System.err.println("Data Loader Worker Completed! Runtime: " + (end-start)/1000 +" seconds");
        System.err.println("Data Loader Worker, Max Realtime Output Queue Size: "+outputQueueMaxSize);

    }

    /**
     * PRICE=8.44, SUPPLIER_NAME=A.G. Edwards Inc., CUSTOMER_NAME=Dominick
     * Smartt, PRODUCT_ID=P-1093, STORE_NAME=Westgate, PRODUCT_NAME=Cinnamon,
     *
     * TRANSACTION_ID=10000, CUSTOMER_ID=C-35, T_DATE=2014-06-17, STORE_ID=S-8,
     * SUPPLIER_ID=SP-3, QUANTITY=5,
     *
     * @param record
     */
    private void processRecord(HashMap record) {

        Long transactionID = (Long) record.get(TransactionBean.TRANSACTION_ID);

        String storeID = (String) record.get(TransactionBean.STORE_ID);;
        String storeName = (String) record.get(TransactionBean.STORE_NAME);

        String customerID = (String) record.get(TransactionBean.CUSTOMER_ID);;
        String customerName = (String) record.get(TransactionBean.CUSTOMER_NAME);

        String supplierID = (String) record.get(MasterBean.SUPPLIER_ID);
        String supplierName = (String) record.get(MasterBean.SUPPLIER_NAME);
        String productID = (String) record.get(MasterBean.PRODUCT_ID);
        String productName = (String) record.get(MasterBean.PRODUCT_NAME);
        Date date = (Date) record.get(TransactionBean.TRANSACTION_DATE);
        Double price = (Double) record.get(MasterBean.PRICE);
        Long quantity = (Long) record.get(TransactionBean.TRANSACTION_QUANTITY);

        if(!storeIDSet.contains(storeID)){
            addOrUpdateStore(storeID, storeName);
        }
        if(!supplierIDSet.contains(supplierID)){
            addOrUpdateSupplier(supplierID, supplierName);
        }
        if(!dateIDSet.contains(date)){
            addOrUpdateDate(date);
        }
        if(!productIDSet.contains(productID)){
            addOrUpdateProduct(productID, productName);
        }
        if(!customerIDSet.contains(customerID)){
            addOrUpdateCustomer(customerID, customerName);
        }

        addSaleTransaction(transactionID, productID, supplierID, customerID, storeID, date, price * quantity);
    }

    private void addOrUpdateStore(String storeID, String storeName) {
        try {
            queryStoreStmt.setString(1, storeID);
            preparedStoreStmt.setString(1, storeID);
            preparedStoreStmt.setString(2, storeName);

        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        executeStatments(queryStoreStmt, preparedStoreStmt);
        storeIDSet.add(storeID);
    }

    private void addOrUpdateProduct(String productID, String productName) {
        try {
            queryProductStmt.setString(1, productID);
            preparedProductStmt.setString(1, productID);
            preparedProductStmt.setString(2, productName);

        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        executeStatments(queryProductStmt, preparedProductStmt);
        productIDSet.add(productID);
    }

    private void addOrUpdateCustomer(String custID, String custName) {
        try {
            queryCustomerStmt.setString(1, custID);
            preparedCustomerStmt.setString(1, custID);
            preparedCustomerStmt.setString(2, custName);

        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        executeStatments(queryCustomerStmt, preparedCustomerStmt);
        customerIDSet.add(custID);
    }

    private void addOrUpdateSupplier(String supplierID, String supplierName) {

        try {
            querySupplierStmt.setString(1, supplierID);
            preparedSupplierStmt.setString(1, supplierID);
            preparedSupplierStmt.setString(2, supplierName);

        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        executeStatments(querySupplierStmt, preparedSupplierStmt);
        supplierIDSet.add(supplierID);
    }

    private void addOrUpdateDate(Date date) {
    
        calendar.setTime(date);
        try {
            queryDateStmt.setDate(1, date);

            preparedDateStmt.setDate(1, date);
            preparedDateStmt.setInt(2, calendar.get(Calendar.DAY_OF_MONTH));
            
            int mm = calendar.get(Calendar.MONTH);
            preparedDateStmt.setInt(3, ++mm);
            
            int quater = 0;
            if (mm <= 3) {
                quater = 1;
            } else if (mm <= 6) {
                quater = 2;
            } else if (mm <= 9) {
                quater = 3;
            } else if (mm <= 12) {
                quater = 4;
            }
            preparedDateStmt.setInt(4, quater);
            preparedDateStmt.setInt(5, calendar.get(Calendar.YEAR));
            preparedDateStmt.setInt(6, calendar.get(Calendar.WEEK_OF_YEAR));

        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        executeStatments(queryDateStmt, preparedDateStmt);
        dateIDSet.add(date);
    }
    
    private void executeStatments(PreparedStatement queryStmt, PreparedStatement updateStmt){
        try {
            boolean needInsert = true;
            ResultSet queryResult = queryStmt.executeQuery();
            if (queryResult.next()) {
                needInsert = false;
            }
            queryResult.close();

            if (needInsert) {
                updateStmt.executeUpdate();
            }
            outConn.commit();
        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     *
     * TRANSACTION_ID BIGINT PRIMARY KEY NOT NULL, --9223372036854775807
     * (java.lang.Long.MAX_VALUE) PRODUCT_ID VARCHAR(6) NOT NULL, SUPPLIER_ID
     * VARCHAR(5) NOT NULL, CUSTOMER_ID VARCHAR(4) NOT NULL, * STORE_ID
     * VARCHAR(4) NOT NULL, DATE_ID DATE NOT NULL, TOTAL_SALE DECIMAL(6,2) NOT
     * NULL, --ASSUME THE PRICE CAN GO HIGH UP TO $9999.99
     */
    private void addSaleTransaction(Long transactionID, String productID, String supplierID, String customerID, String storeID, Date date, double totalPrice) {
        
        try {
            boolean needInsert = true;
            
            queryTransStmt.setLong(1, transactionID);
            ResultSet queryResult = queryTransStmt.executeQuery();
            if (queryResult.next()) {
                needInsert = false;
            }
            queryResult.close();
            
            outConn.commit();
            if (needInsert) {
                inisertTransactionInBatch(transactionID, productID, supplierID, customerID, storeID, date, totalPrice);
            }
            
        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
        
    private void inisertTransactionInBatch(Long transactionID, String productID, String supplierID, String customerID, String storeID, Date date, double totalPrice) {
        currentBatchCount++;

        try {
            transInsertStmt.setLong(1, transactionID);
            transInsertStmt.setString(2, productID);
            transInsertStmt.setString(3, supplierID);
            transInsertStmt.setString(4, customerID);
            transInsertStmt.setString(5, storeID);
            transInsertStmt.setDate(6, date);
            transInsertStmt.setDouble(7, totalPrice);
            transInsertStmt.addBatch();

        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (currentBatchCount >= batchStmtCount) {
            commitTransBatch();
        }
    }

    private void commitTransBatch() {
        try {
            transInsertStmt.executeBatch();
            outConn.commit();
            currentBatchCount = 0;
        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
