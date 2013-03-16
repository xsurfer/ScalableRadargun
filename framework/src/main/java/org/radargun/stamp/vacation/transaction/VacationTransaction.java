package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;

public abstract class VacationTransaction {

    public abstract void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;
    
    public abstract boolean isReadOnly();
}
