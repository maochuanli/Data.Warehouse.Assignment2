/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown;

import com.countdown.bean.pool.MasterBean;
import com.countdown.bean.pool.TransactionBean;
import com.countdown.db.DBManager;
import com.countdown.thread.RealtimeDataQueue;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maochuanli
 */
public class MeshjoinWorker {
    private MeshjoinWorker(){}
    private static MeshjoinWorker INSTANCE = new MeshjoinWorker();
    private ArrayList<HashMap> meshHashTableUsingList = new ArrayList();
    private HashMap<String, HashMap> masterBufferTable = new HashMap();
    
    private boolean isSetup = false;
    private Connection dbConn;
    private final String QueryCountSQL = "select count(*) as many from masterdata";
    private final String QueryPartitionSQL = "select * from masterdata order by product_id";
    private int masterRecordCount = 0;
    private int masterPartitionSize = 0;
    private final int partitions = StreamPartitionQueue.partitions;
    private int currentMasterPartitionIndex = 0;
    private int loadedPartitions = 0;
    
    public static void processMeshJoin(StreamPartition newAddedPartition){
        //step0, if database connection not set, set it up
        if(INSTANCE.isSetup == false){
            INSTANCE.setup();
            INSTANCE.isSetup = true;
        }
        
        //step1, add all records iin new partition into the hash table
        List<HashMap> newAddedTupleList = newAddedPartition.getPartitionTupleList();
        INSTANCE.info("MeshjoinWorker WARN: partition size: "+newAddedTupleList.size());
        INSTANCE.info("Meshjoinworker process partition ID..." + newAddedPartition.getPartitionID());
        
        
        for(HashMap record: newAddedTupleList){
            INSTANCE.meshHashTableUsingList.add(record);
//            Long id = (Long) record.get(TransactionBean.TRANSACTION_ID);
//            
//            System.err.print(id+",");
            
        }
//        System.err.println();
        
        //step2, load one partition from the master data
        INSTANCE.loadMasterPartition();
        //increase the partition index
        INSTANCE.loadedPartitions++;
        
        //step3, match all partition master tuples with all records in hash table, and create new tuples, and add them to output queue
        INSTANCE.meshjoin();
        //step4, delete the old partition in the stream partition queue
        INSTANCE.cleanupPartition();
        
        //increase the partition index
        
        INSTANCE.currentMasterPartitionIndex++;
        if(INSTANCE.currentMasterPartitionIndex >= INSTANCE.partitions){
            INSTANCE.currentMasterPartitionIndex = 0;
        }
    }

    private void loadMasterPartition() {
        
        //return the obj
        for(HashMap r: masterBufferTable.values()){
            try {
                Main.getObjectPool().returnObject(r);
            } catch (Exception ex) {
                Logger.getLogger(MeshjoinWorker.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        masterBufferTable.clear();
        
        try {
            Statement stat = dbConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                    ResultSet.CONCUR_READ_ONLY);
            ResultSet masterResult = stat.executeQuery(QueryPartitionSQL);
            
            int skipCount = this.currentMasterPartitionIndex * masterPartitionSize;
            masterResult.absolute(skipCount);
            
            int i = 0;
            while(masterResult.next() ){
                processMasterRecord(masterResult);                
                if(++i>=masterPartitionSize) break;
            }
            
            
            if( (partitions - currentMasterPartitionIndex - 1) == 0 
                    && !masterResult.isAfterLast()){//the last partition, fetch all left rows
                while(masterResult.next()){
                    processMasterRecord(masterResult);
                }
            }
            
            stat.close();
        } catch (SQLException ex) {
            Logger.getLogger(MeshjoinWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }

    private void setup() {
        this.dbConn = DBManager.getInConnection(DBManager.IN_MASTER);
        try {
            Statement stat = dbConn.createStatement();
            ResultSet countResult = stat.executeQuery(QueryCountSQL);
            countResult.next();
            this.masterRecordCount = countResult.getInt(1);
            this.masterPartitionSize = this.masterRecordCount / partitions;
            stat.close();
        } catch (SQLException ex) {
            Logger.getLogger(MeshjoinWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void tearDown() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
        
    private void processMasterRecord(ResultSet masterResult) {
        try {
            HashMap record = Main.getObjectPool().borrowObject();
            String pid = masterResult.getString(MasterBean.PRODUCT_ID);
            
            record.put(MasterBean.PRICE, masterResult.getDouble(MasterBean.PRICE));
            record.put(MasterBean.PRODUCT_ID, pid);
            record.put(MasterBean.PRODUCT_NAME, masterResult.getString(MasterBean.PRODUCT_NAME));
            record.put(MasterBean.SUPPLIER_ID, masterResult.getString(MasterBean.SUPPLIER_ID));
            record.put(MasterBean.SUPPLIER_NAME, masterResult.getString(MasterBean.SUPPLIER_NAME));
            
            this.masterBufferTable.put(pid,record);
//            System.out.println("PID = "+ pid);
        } catch (Exception ex) {
            Logger.getLogger(MeshjoinWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }

    private void meshjoin() {
        for(HashMap streamRecord: this.meshHashTableUsingList){
            
            if(streamRecord.get("joined") != null) continue;
            
            String streamRecordPid = (String) streamRecord.get(TransactionBean.PRODUCT_ID);
            HashMap masterRecord = this.masterBufferTable.get(streamRecordPid);
            
            if(masterRecord != null){
                HashMap joinRecord;
                try {
                    joinRecord = Main.getObjectPool().borrowObject();
                    joinRecord.putAll(streamRecord);
                    joinRecord.putAll(masterRecord);
                    
                    streamRecord.put("joined", "true");
                    
                    RealtimeDataQueue.getOutputDataQueue().add(joinRecord);
                } catch (Exception ex) {
                    Logger.getLogger(MeshjoinWorker.class.getName()).log(Level.SEVERE, null, ex);
                }
                 
            }
        }
    }

    private void cleanupPartition() {
        info("currentMasterPartitionIndex=" + currentMasterPartitionIndex+",loadedPartitions="+loadedPartitions+",partitions="+partitions);
        if (loadedPartitions > partitions) {
            StreamPartition pollPartition = StreamPartitionQueue.pollPartition();
            info("MeshjoinWorker clean up poll partition: " + pollPartition.getPartitionID());
            if (pollPartition != null) {
                List<HashMap> partitionTupleList = pollPartition.getPartitionTupleList();
                for (HashMap partitionTuple : partitionTupleList) {

                    if (partitionTuple.get("joined") == null) {
                        System.err.println("Critical Error! Product ID: " + partitionTuple.get("PRODUCT_ID") + ", TRAN_ID=" + partitionTuple.get(TransactionBean.TRANSACTION_ID) + " NOT matched!");
                    }

                    meshHashTableUsingList.remove(partitionTuple);
                    try {
                        Main.getObjectPool().returnObject(partitionTuple);
                    } catch (Exception ex) {
                        Logger.getLogger(MeshjoinWorker.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

            }
        }
        
        
    }
    
    void info(String msg){
        Logger.getLogger(MeshjoinWorker.class.getName()).log(Level.INFO, msg);
    }
}
