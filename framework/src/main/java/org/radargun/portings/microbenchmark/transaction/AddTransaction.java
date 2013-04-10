package org.radargun.portings.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.portings.microbenchmark.domain.IntSet;

public class AddTransaction implements MicrobenchmarkTransaction {

    public final int value;

    public AddTransaction(int value) {
        this.value = value;
    }

    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
        IntSet set = (IntSet)cacheWrapper.get(null, "SET");
        boolean res = set.add(cacheWrapper, this.value);
        return res;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

}
