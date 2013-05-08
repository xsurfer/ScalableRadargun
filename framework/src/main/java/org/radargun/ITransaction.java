package org.radargun;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 5/8/13
 */
public interface ITransaction {

    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;

    public boolean isReadOnly();

    public int getType();
}
