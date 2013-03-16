package org.radargun.ycsb;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stressors.AbstractCacheWrapperStressor;
import org.radargun.ycsb.transaction.Insert;

public class YCSBPopulationStressor extends AbstractCacheWrapperStressor {
    
    private static Log log = LogFactory.getLog(YCSBPopulationStressor.class);
    
    private CacheWrapper wrapper;
    private int recordCount;
    
    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
	if (wrapper == null) {
	    throw new IllegalStateException("Null wrapper not allowed");
	}
	try {
	    log.info("Performing Population Operations");
	    this.wrapper = wrapper;
	    performPopulation();
	} catch (Exception e) {
	    log.warn("Received exception during cache population" + e.getMessage());
	    e.printStackTrace();
	}
	return null;
    }

    private void performPopulation() {
	boolean successful = false;
	while (!successful) {
	    try {
		wrapper.startTransaction(false);

		for (int i = 0; i < recordCount; i++) {
		   new Insert(i).executeTransaction(wrapper);
		}

		wrapper.endTransaction(true);
		successful = true;
		
	    } catch (Throwable e) {
		System.out.println("Exception during population, going to rollback after this");
		e.printStackTrace();
		log.warn(e);
		try {
		    wrapper.endTransaction(false);
		} catch (Throwable e2) {
		    System.out.println("Exception during rollback!");
		    e2.printStackTrace();
		}
	    }
	}
    }

    @Override
    public void destroy() throws Exception {
	//Don't destroy data in cache!
    }
    
    

}
