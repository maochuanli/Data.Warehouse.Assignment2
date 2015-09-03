/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.db;

import com.countdown.MainSystem;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maochuanli
 */
public class DBManager {
    private DBManager(){}
//    private static DBManager INSTANCE = new DBManager();
    private static Connection inTransConn, inMasterConn, outConn;
    public static final String IN_TRANS = "in_transaction_conn";
    public static final String IN_MASTER = "in_transaction_conn";
    
    public static synchronized Connection getInConnection(String flags){
        if(inTransConn==null){
            Properties props = new Properties(); // connection properties
            props.put("user", MainSystem.getInDBUser());
            props.put("password", MainSystem.getInDBPass());
            String dbURL = MainSystem.getInDBURL();
            
            try {
                Class.forName(MainSystem.getInDBDriver());
                inTransConn = DriverManager.getConnection(dbURL, props);
                inMasterConn = DriverManager.getConnection(dbURL, props);
                System.out.println("Connected to (and created) Operational database " + dbURL);
//                inTransConn.setAutoCommit(false);
            } catch (SQLException ex) {
                printSQLException(ex);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(DBManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if(flags.equals(IN_MASTER)){
            return inMasterConn;
        }
        return inTransConn;
    }
    
    public static synchronized void closeInConnection(String flags){
        try {
            if(flags.equals(IN_TRANS)){
                if (inTransConn != null) {
                    inTransConn.close();
                    inTransConn = null;
                }
            } else {
                if (inMasterConn != null) {
                    inMasterConn.close();
                    inMasterConn = null;
                }
            }
            
        } catch (SQLException sqle) {
            printSQLException(sqle);
        }
        
//        String driverClass = MainSystem.getInDBDriver();
//        if(driverClass.contains("EmbeddedDriver")){
//            try
//                {
//                    // the shutdown=true attribute shuts down Derby
//                DriverManager.getConnection("jdbc:derby:;shutdown=true");
//
//                    // To shut down a specific database only, but keep the
//                    // engine running (for example for connecting to other
//                    // databases), specify a database in the connection URL:
//                    //DriverManager.getConnection("jdbc:derby:" + dbName + ";shutdown=true");
//                }
//                catch (SQLException se)
//                {
//                    if (( (se.getErrorCode() == 50000)
//                            && ("XJ015".equals(se.getSQLState()) ))) {
//                        // we got the expected exception
//                        logMsg("Derby shut down normally");
//                        // Note that for single database shutdown, the expected
//                        // SQL state is "08006", and the error code is 45000.
//                    } else {
//                        // if the error code or SQLState is different, we have
//                        // an unexpected exception (shutdown failed)
//                        logMsg("Derby did not shut down normally");
//                        printSQLException(se);
//                    }
//                }
//        }
    }
    
    /**
     * For Data warehouse database 
     * @return  
     */
    public static synchronized Connection getOutConnection(){
        if(outConn==null){
            Properties props = new Properties(); // connection properties
            // providing a user name and password is optional in the embedded
            // and derbyclient frameworks
            props.put("user", MainSystem.getOutDBUser());
            props.put("password", MainSystem.getOutDBPass());
            String dbURL = MainSystem.getOutDBURL();
            
            try {

                Class.forName(MainSystem.getOutDBDriver());

                // We want to control transactions manually. Autocommit is on by
                // default in JDBC.
                outConn = DriverManager.getConnection(dbURL, props);

                System.out.println("Connected to (and created) Data Warehouse database " + dbURL);
                outConn.setAutoCommit(false);
            } catch (SQLException ex) {
                printSQLException(ex);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(DBManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return outConn;
    }
    
    public static synchronized void closeOutConnection(){
        try {
            if (outConn != null) {
                outConn.commit();
                outConn.close();
                outConn = null;
            }
        } catch (SQLException sqle) {
            printSQLException(sqle);
        }
        
    }
    
    public static void printSQLException(SQLException e)
    {
//        while (e != null){
//        
//            logMsg("\n----- SQLException -----");
//            logMsg("  SQL State:  " + e.getSQLState());
//            logMsg("  Error Code: " + e.getErrorCode());
//            logMsg("  Message:    " + e.getMessage());
//            // for stack traces, refer to derby.log or uncomment this:
//            //e.printStackTrace(System.err);
//            e = e.getNextException();
//        }
        e.printStackTrace();
    }
    
    
    public static void logMsg(String msg){
        Logger.getLogger(DBManager.class.getName()).log(Level.SEVERE, msg, (Throwable)null);
    }
    
    public static void logMsg(String msg, Exception e){
        Logger.getLogger(DBManager.class.getName()).log(Level.SEVERE, msg, e);
    }
}
