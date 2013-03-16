package org.radargun.ycsb;

import org.radargun.CacheWrapper;
import org.radargun.ycsb.generators.IntegerGenerator;
import org.radargun.ycsb.generators.ZipfianGenerator;

public class YCSB {

    public static int fieldcount = 10;
    public static int readOnly;
    public static IntegerGenerator fieldlengthgenerator;

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

    public static void preinit() {
	int fieldlength= 100;
	fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
    }
    
    public static void init(int readOnly, int recordCount) {
        YCSB.readOnly = readOnly;
    }


}
