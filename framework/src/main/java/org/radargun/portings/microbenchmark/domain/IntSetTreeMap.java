package org.radargun.portings.microbenchmark.domain;

import org.radargun.CacheWrapper;

public class IntSetTreeMap extends TreeMapJvstm<Integer,Object> implements IntSet {
    
    public IntSetTreeMap(CacheWrapper cache) {
        super(cache, -1, new Object());
    }

    private static final Object PRESENT = new Object();

    public boolean add(CacheWrapper wrapper, final int value) {
        return super.put(wrapper, value, PRESENT) == null;
    }

    public boolean remove(CacheWrapper wrapper, final int value) {
        return super.remove(wrapper, value) != null;
    }

    public boolean contains(CacheWrapper wrapper, final int value) {
        return super.containsKey(wrapper, value);
    }
}

