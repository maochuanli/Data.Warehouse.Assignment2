/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.countdown.bean.pool;

import java.util.HashMap;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

    
/**
 *
 * @author maochuanli
 */
public class RecordFactory extends BasePooledObjectFactory<HashMap> {
    
    @Override
    public HashMap create() throws Exception {
        return new HashMap();
    }

    
    @Override
    public PooledObject<HashMap> wrap(HashMap t) {
        return new DefaultPooledObject<>(t);
    }
    
    /**
     * When an object is returned to the pool, clear the buffer.
     * @param pooledObject
     */
    @Override
    public void passivateObject(PooledObject<HashMap> pooledObject) {
        pooledObject.getObject().clear();
    }
}
