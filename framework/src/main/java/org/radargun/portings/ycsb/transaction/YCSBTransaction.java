package org.radargun.portings.ycsb.transaction;

import org.radargun.CacheWrapper;

public abstract class YCSBTransaction {

   public abstract void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;

   public abstract boolean isReadOnly();
}
    
