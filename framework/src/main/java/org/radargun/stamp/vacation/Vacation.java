package org.radargun.stamp.vacation;

import org.radargun.CacheWrapper;

public class Vacation {

    public static final void put(CacheWrapper cacheWrapper, Object key, Object value) {
	try {
	    cacheWrapper.put(null, key, value);
	} catch (Exception e) {
	    if (e instanceof RuntimeException) {
		throw (RuntimeException)e;
	    }
	    e.printStackTrace();
	}
    }
    
    public static final <T> T get(CacheWrapper cacheWrapper, Object key) {
	try {
	    return (T) cacheWrapper.get(null, key);
	} catch (Exception e) {
	    if (e instanceof RuntimeException) {
		throw (RuntimeException)e;
	    }
	    e.printStackTrace();
	    return null;
	}
    }
    
}
