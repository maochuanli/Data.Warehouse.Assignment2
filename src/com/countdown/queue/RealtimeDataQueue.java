/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.queue;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author maochuanli
 */
public class RealtimeDataQueue {
    private final ConcurrentLinkedQueue iQueue = new ConcurrentLinkedQueue();
    private final ConcurrentLinkedQueue outQueue = new ConcurrentLinkedQueue();
    private RealtimeDataQueue(){}
    private static final RealtimeDataQueue INSTANCE = new RealtimeDataQueue();
    
    
    public static ConcurrentLinkedQueue getInputDataQueue(){
        return INSTANCE.iQueue;
    }
    public static ConcurrentLinkedQueue getOutputDataQueue(){
        return INSTANCE.outQueue;
    }    
}
