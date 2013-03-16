package org.radargun.microbenchmark.domain;

import org.radargun.CacheWrapper;

public interface IntSet {

    public boolean add(CacheWrapper cache, int value);
    
    public boolean remove(CacheWrapper cache, int value);

    public boolean contains(CacheWrapper cache, int value);
    
}
