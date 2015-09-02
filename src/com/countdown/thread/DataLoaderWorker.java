/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.thread;

import com.countdown.bean.pool.TransactionBean;
import com.countdown.db.DBManager;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
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
public class DataLoaderWorker extends Thread{

    private volatile boolean running = true;
    private Queue realtimeOutputQueue = null;
    private Connection outConn = null;
    private HashSet<Long> idSet = new HashSet<>();
    
    public boolean setup(){
        realtimeOutputQueue = RealtimeDataQueue.getOutputDataQueue();
        outConn = DBManager.getOutConnection();
        if(outConn != null){
            try {
                return outConn.isValid(0);
            } catch (SQLException ex) {
                Logger.getLogger(RealtimeDataProducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return false;
    }
    
    private void tearDown() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
        
        Object[] toArray = idSet.toArray();
        Arrays.sort(toArray);
        System.err.println("Data Loader Worker Completed! "+ idSet.size()); //
        
        
    }

    /**
     * PRICE=8.44,
     * SUPPLIER_NAME=A.G. Edwards Inc.,
     * CUSTOMER_NAME=Dominick Smartt,
     * PRODUCT_ID=P-1093,
     * STORE_NAME=Westgate,
     * PRODUCT_NAME=Cinnamon,
     * 
     * TRANSACTION_ID=10000,
     * CUSTOMER_ID=C-35,
     * T_DATE=2014-06-17,
     * STORE_ID=S-8,
     * SUPPLIER_ID=SP-3,
     * QUANTITY=5,
     * @param record 
     */
    private void processRecord(HashMap record) {
        
                
        Long transactionID = (Long) record.get(TransactionBean.TRANSACTION_ID);
//        idSet.add(transactionID);
//        if(transactionID==10000){
//            StringBuilder b = new StringBuilder();
//            for (Object key : record.keySet()) {
//                b.append(key + "=" + record.get(key) + ",");
//            }
            
            String storeID = (String) record.get("STORE_ID");;
            String storeName = (String) record.get("STORE_NAME");

            String customerID = (String) record.get("CUSTOMER_ID");;
            String customerName = (String) record.get("CUSTOMER_NAME");

            String supplierID = (String) record.get("SUPPLIER_ID");
            String supplierName = (String) record.get("SUPPLIER_NAME");
            String productID = (String) record.get("PRODUCT_ID");
            String productName = (String) record.get("PRODUCT_NAME");
            Date date = (Date) record.get("T_DATE");
            Double price = (Double) record.get("PRICE");
            Long quantity = (Long) record.get("QUANTITY");

            addOrUpdateStore(storeID, storeName);
            addOrUpdateSupplier(supplierID, supplierName);
            addOrUpdateDate(date);
            addOrUpdateProduct(productID, productName);
            addOrUpdateCustomer(customerID,customerName);
            
            addTransaction(transactionID,productID,supplierID,customerID,storeID,date,price*quantity);
            
//            System.out.println("Record: " + b.toString());
//        }
    }

    private void addOrUpdateStore(String storeID, String storeName) {
        String querySQL = "select * from STORES where STORE_ID='"+storeID+"'";
        String updateSQL = "insert into stores values( '"+storeID+"','"+storeName+"')";
        
        addUpdateRecord(querySQL, updateSQL);
    }

    private void addOrUpdateSupplier(String supplierID, String supplierName) {
        String querySQL = "select * from SUPPLIERS where SUPPLIER_ID=?";
        String updateSQL = "insert into SUPPLIERS(SUPPLIER_ID,SUPPLIER_NAME) values(?,?)";
        
        try {
            boolean needInsert = true;
            PreparedStatement queryStoreStmt = outConn.prepareStatement(querySQL);
            queryStoreStmt.setString(1, supplierID);
            ResultSet queryResult = queryStoreStmt.executeQuery();
            if(queryResult.next()){
//                info("date record existed.");
                needInsert = false; 
            }
            queryResult.close();
            queryStoreStmt.close();
            
            if(needInsert){
                PreparedStatement preparedStatement = outConn.prepareStatement(updateSQL);
                preparedStatement.setString(1, supplierID);
                preparedStatement.setString(2, supplierName);
                
                preparedStatement.executeUpdate();
                preparedStatement.close();
                outConn.commit();
//                info("date record inserted.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void addOrUpdateDate(Date date) {
        String querySQL = "select * from DATES where DATE_ID=?";
        String updateSQL = "INSERT INTO DATES"
                + "(DATE_ID, DD, MM, QTR, YYYY, WEEK) VALUES"
                + "(?,?,?,?,?,?)";
        Calendar cal = new GregorianCalendar();
        cal.setTime(date);
        try {
            boolean needInsert = true;
            PreparedStatement queryStoreStmt = outConn.prepareStatement(querySQL);
            queryStoreStmt.setDate(1, date);
            ResultSet queryResult = queryStoreStmt.executeQuery();
            if(queryResult.next()){
//                info("date record existed.");
                needInsert = false; 
            }
            queryResult.close();
            queryStoreStmt.close();
            
            if(needInsert){
                PreparedStatement preparedStatement = outConn.prepareStatement(updateSQL);
                preparedStatement.setDate(1, date);
                preparedStatement.setInt(2, cal.get(Calendar.DAY_OF_MONTH));
                int mm = cal.get(Calendar.MONTH);
                preparedStatement.setInt(3, ++mm);
                int quatuer = 0;
                if(mm <= 3){
                    quatuer = 1;
                }else if(mm <= 6){
                    quatuer = 2;
                }else if(mm <= 9){
                    quatuer = 3;
                }else if(mm <= 12){
                    quatuer = 4;
                }
                preparedStatement.setInt(4, quatuer);
                preparedStatement.setInt(5, cal.get(Calendar.YEAR));
                preparedStatement.setInt(6, cal.get(Calendar.WEEK_OF_YEAR));
                
                preparedStatement.executeUpdate();
                preparedStatement.close();
                outConn.commit();
//                info("date record inserted.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
    }

    private void addOrUpdateProduct(String productID, String productName) {
        String querySQL = "select * from PRODUCTS where PRODUCT_ID='"+productID+"'";
        String updateSQL = "insert into PRODUCTS values( '"+productID+"','"+productName+"')";
        
        addUpdateRecord(querySQL, updateSQL);
    }
    
    
    private void addOrUpdateCustomer(String custID, String custName) {
        String querySQL = "select * from CUSTOMERS where CUSTOMER_ID='"+custID+"'";
        String updateSQL = "insert into CUSTOMERS values( '"+custID+"','"+custName+"')";
        
        addUpdateRecord(querySQL, updateSQL);
    }

    private void addUpdateRecord(String querySQL, String updateSQL) {
        try {
            boolean needInsert = true;
            Statement queryStoreStmt = outConn.createStatement();
            ResultSet queryResult = queryStoreStmt.executeQuery(querySQL);
            if(queryResult.next()){
//                info("record existed.");
                needInsert = false; 
            }
            queryResult.close();
            queryStoreStmt.close();
            
            if(needInsert){
                Statement updateStatement = outConn.createStatement();
                
                updateStatement.executeUpdate(updateSQL);
                updateStatement.close();
                outConn.commit();
//                info("record inserted.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
        
    void info(String msg){
        System.out.println(msg);
    }
    /**
             *     
    TRANSACTION_ID BIGINT PRIMARY KEY NOT NULL, --9223372036854775807 (java.lang.Long.MAX_VALUE)
    PRODUCT_ID VARCHAR(6) NOT NULL, 
    SUPPLIER_ID VARCHAR(5) NOT NULL, 
    CUSTOMER_ID VARCHAR(4) NOT NULL, 
     
    STORE_ID VARCHAR(4) NOT NULL, 
    DATE_ID DATE NOT NULL, 
    TOTAL_SALE DECIMAL(6,2) NOT NULL, --ASSUME THE PRICE CAN GO HIGH UP TO $9999.99
             */
    private void addTransaction(Long transactionID, String productID, String supplierID, String customerID, String storeID, Date date, double totalPrice) {
        String querySQL = "select * from SALES where TRANSACTION_ID=?";
        String updateSQL = "INSERT INTO SALES"
                + "(TRANSACTION_ID, PRODUCT_ID, SUPPLIER_ID, CUSTOMER_ID, STORE_ID, DATE_ID, TOTAL_SALE) VALUES"
                + "(?,?,?,?,?,?,?)";
        Calendar cal = new GregorianCalendar();
        cal.setTime(date);
        try {
            boolean needInsert = true;
            PreparedStatement queryStoreStmt = outConn.prepareStatement(querySQL);
            queryStoreStmt.setLong(1, transactionID);
            ResultSet queryResult = queryStoreStmt.executeQuery();
            if(queryResult.next()){
//                info("date record existed.");
                needInsert = false; 
            }
            queryResult.close();
            queryStoreStmt.close();
            
            if(needInsert){
                PreparedStatement preparedStatement = outConn.prepareStatement(updateSQL);
                preparedStatement.setLong(1, transactionID);
                preparedStatement.setString(2, productID);
                preparedStatement.setString(3, supplierID);
                preparedStatement.setString(4, customerID);
                preparedStatement.setString(5, storeID);
                preparedStatement.setDate(6, date);
                preparedStatement.setDouble(7, totalPrice);
                preparedStatement.executeUpdate();
                preparedStatement.close();
                outConn.commit();
//                info("date record inserted.");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataLoaderWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
