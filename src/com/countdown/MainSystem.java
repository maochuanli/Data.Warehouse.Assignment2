/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author maochuanli
 */
public class MainSystem {

    private static final String systemFileName = "system.properties";
    private static final Properties settings = new Properties();
    private static boolean loaded = false;

    static {
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(systemFileName);
            settings.load(fin);
            loaded = true;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(MainSystem.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(MainSystem.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                fin.close();
            } catch (Exception ex) {
                Logger.getLogger(MainSystem.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static boolean isReady() {
        return loaded;
    }

    public static String getInDBDriver() {
        return settings.getProperty("in.db.driverClassName");
    }

    public static String getInDBURL() {
        return settings.getProperty("in.db.url");
    }

    public static String getInDBUser() {
        return settings.getProperty("in.db.username");
    }

    public static String getInDBPass() {
        return settings.getProperty("in.db.password");
    }

    public static String getOutDBDriver() {
        return settings.getProperty("out.db.driverClassName");
    }

    public static String getOutDBURL() {
        return settings.getProperty("out.db.url");
    }

    public static String getOutDBUser() {
        return settings.getProperty("out.db.username");
    }

    public static String getOutDBPass() {
        return settings.getProperty("out.db.password");
    }

    public static int getMeshjoinPartitions() {
        String is = settings.getProperty("meshjoin.partitions");
        return Integer.parseInt(is);
    }

    public static int getPartitionSize() {
        String is = settings.getProperty("stream.partition.size");
        return Integer.parseInt(is);
    }

    public static int getInsertBatchSize() {
        String is = settings.getProperty("sql.insert.batch.size");
        return Integer.parseInt(is);
    }
}
