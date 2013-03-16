package org.radargun.ycsb.transaction;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.VacationStressor;
import org.radargun.ycsb.YCSBStressor;

public abstract class YCSBTransaction {

    public abstract void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;
    
    public abstract boolean isReadOnly();
}
    
