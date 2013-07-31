package org.radargun.portings.ycsb.transaction;

import org.radargun.CacheWrapper;
import org.radargun.portings.ycsb.ByteIterator;
import org.radargun.portings.ycsb.RandomByteIterator;
import org.radargun.portings.ycsb.StringByteIterator;
import org.radargun.portings.ycsb.YCSB;

import java.util.HashMap;
import java.util.Map;

public class Insert extends YCSBTransaction {

    private int k;
    
    public Insert(int k) {
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
	Map<String, String> row = StringByteIterator.getStringMap(values);
	cacheWrapper.put(null, "user" + k, row);
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }

}
