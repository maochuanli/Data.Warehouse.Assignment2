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
                System.out.println("Connected to (and created) Operational database " + dbURL);

            } catch (SQLException ex) {
                printSQLException(ex);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(DBManager.class.getName()).log(Level.SEVERE, null, ex);
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

    public static synchronized void closeOutConnection() {
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

    public static void printSQLException(SQLException e) {
        while (e != null) {

            logMsg("\n----- SQLException -----");
            logMsg("  SQL State:  " + e.getSQLState());
            logMsg("  Error Code: " + e.getErrorCode());
            logMsg("  Message:    " + e.getMessage());
        }
    }

    public static void logMsg(String msg) {
        Logger.getLogger(DBManager.class.getName()).log(Level.SEVERE, msg, (Throwable) null);
    }

    public static void logMsg(String msg, Exception e) {
        Logger.getLogger(DBManager.class.getName()).log(Level.SEVERE, msg, e);
    }
}
