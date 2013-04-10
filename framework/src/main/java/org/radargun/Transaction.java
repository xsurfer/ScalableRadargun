package org.radargun;

/**
 * @author perfabio87@gmail.com
 */
public interface Transaction {

   void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;

   boolean isReadOnly();

   int getType();

}
