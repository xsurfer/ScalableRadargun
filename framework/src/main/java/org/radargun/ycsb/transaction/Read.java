package org.radargun.ycsb.transaction;

import java.util.HashMap;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.ycsb.ByteIterator;
import org.radargun.ycsb.RandomByteIterator;
import org.radargun.ycsb.StringByteIterator;
import org.radargun.ycsb.YCSB;

public class Read extends YCSBTransaction {

    private int k;
    
    public Read(int k) {
	this.k = k;
    }

    @Override
    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
	HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

	for (int i=0; i< YCSB.fieldcount; i++)
	{
	    String fieldkey="field"+i;
	    ByteIterator data= new RandomByteIterator(YCSB.fieldlengthgenerator.nextInt());
	    values.put(fieldkey,data);
	}
	Map<String, String> row = (Map) cacheWrapper.get(null, "user" + k);
	HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
	if (row != null) {
	    result.clear();
	    StringByteIterator.putAllAsByteIterators(result, row);
	}
    }

    @Override
    public boolean isReadOnly() {
	return true;
    }
}
