package org.radargun.portings.ycsb.transaction;

import java.util.HashMap;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.portings.ycsb.ByteIterator;
import org.radargun.portings.ycsb.RandomByteIterator;
import org.radargun.portings.ycsb.StringByteIterator;
import org.radargun.portings.ycsb.YCSB;

public class RMW extends YCSBTransaction {

    private int k;
    private int multiplereadcount;
    private int random;
    private int recordCount;
    
    public RMW(int k, int random, int multiplereadcount, int recordCount) {
	this.random = Math.abs(random);
	this.k = k;
	this.multiplereadcount = multiplereadcount;
	this.recordCount = recordCount;
    }

    @Override
    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
	HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

	for (int i=0; i< YCSB.fieldcount; i++) {
	    String fieldkey="field"+i;
	    ByteIterator data= new RandomByteIterator(YCSB.fieldlengthgenerator.nextInt());
	    values.put(fieldkey,data);
	}
	
	Map<String, String> row = StringByteIterator.getStringMap(values);
	int toWrite = (Math.abs(random)) % multiplereadcount;
	for (int i = 0 ; i < multiplereadcount; i++) {
	    if (toWrite == i) {
		cacheWrapper.put(null, "user" + ((k + i) % recordCount), row);
	    } else {
		cacheWrapper.get(null, "user" + ((k + i) % recordCount));
	    }
	}
	
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }
}
