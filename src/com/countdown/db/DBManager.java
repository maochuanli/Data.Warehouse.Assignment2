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

    private DBManager() {
    }
    private static Logger LOGGER = Logger.getLogger(DBManager.class.getName());
    
    private static Connection inTransConn, inMasterConn, outConn;
    public static final String IN_TRANS = "in_transaction_conn";
    public static final String IN_MASTER = "in_transaction_conn";

    public static synchronized Connection getInConnection(String flags) {
        if (inTransConn == null) {
            Properties props = new Properties(); // connection properties
            props.put("user", MainSystem.getInDBUser());
            props.put("password", MainSystem.getInDBPass());
            String dbURL = MainSystem.getInDBURL();

            try {
                Class.forName(MainSystem.getInDBDriver());
                inTransConn = DriverManager.getConnection(dbURL, props);
                inMasterConn = DriverManager.getConnection(dbURL, props);
                LOGGER.info("Connected to Operational databases " + dbURL);

            } catch (SQLException ex) {
                printSQLException(ex);
            } catch (ClassNotFoundException ex) {
                LOGGER.log(Level.SEVERE, null, ex);
            }
        }
        if (flags.equals(IN_MASTER)) {
            return inMasterConn;
        }
        return inTransConn;
    }

    public static synchronized void closeInConnection(String flags) {
        try {
            if (flags.equals(IN_TRANS)) {
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
            LOGGER.info("Connection for input database ["+flags+"] is closed!");
        } catch (SQLException sqle) {
            printSQLException(sqle);
        }
    }

    /**
     * For Data warehouse database
     *
     * @return
     */
    public static synchronized Connection getOutConnection() {
        if (outConn == null) {
            Properties props = new Properties(); // connection properties
            
            props.put("user", MainSystem.getOutDBUser());
            props.put("password", MainSystem.getOutDBPass());
            String dbURL = MainSystem.getOutDBURL();

            try {
                Class.forName(MainSystem.getOutDBDriver());
                outConn = DriverManager.getConnection(dbURL, props);
                LOGGER.info("Connected to Data Warehouse database " + dbURL);
                outConn.setAutoCommit(false);
            } catch (SQLException ex) {
                printSQLException(ex);
            } catch (ClassNotFoundException ex) {
                LOGGER.log(Level.SEVERE, null, ex);
            }
        }
        return outConn;
    }

    public static synchronized void closeOutConnection() {
        try {
            if (outConn != null) {
                outConn.commit();
                outConn.close();
                outConn = null;
            }
            LOGGER.info("Connection to output Data Warehouse is closed!");
        } catch (SQLException sqle) {
            printSQLException(sqle);
        }

    }

    public static void printSQLException(SQLException e) {
        while (e != null) {
            LOGGER.severe("\n----- SQLException -----");
            LOGGER.severe("  SQL State:  " + e.getSQLState());
            LOGGER.severe("  Error Code: " + e.getErrorCode());
            LOGGER.severe("  Message:    " + e.getMessage());
        }
    }
}
