package org.radargun.portings.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.portings.microbenchmark.domain.IntSet;

public class ContainsTransaction implements MicrobenchmarkTransaction {

    public final int value;

    public ContainsTransaction(int value) {
        this.value = value;
    }

    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
	IntSet intset = ((IntSet)cacheWrapper.get(null, "SET"));
        return intset.contains(cacheWrapper, this.value);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }
}
