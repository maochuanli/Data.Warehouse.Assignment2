/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.thread.meshjoin;

import com.countdown.MainSystem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author maochuanli
 */
public class StreamPartition {
    private final long partitionID;
    private final int partitionSize;
    private final List<HashMap> partitionTupleList = new ArrayList<HashMap>();

    public StreamPartition(long partitionID) {
        this.partitionSize = MainSystem.getPartitionSize();
        this.partitionID = partitionID;
    }

    public long getPartitionID() {
        return partitionID;
    }

    public List<HashMap> getPartitionTupleList() {
        return partitionTupleList;
    }

    public boolean isFull() {
        return this.partitionSize <= partitionTupleList.size();
    }

    void addRecord(HashMap record) {
        this.partitionTupleList.add(record);
    }
    
}
