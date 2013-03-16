package org.radargun.stages;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.microbenchmark.MicrobenchmarkStressor;
import org.radargun.state.MasterState;

public class MicrobenchmarkStage extends AbstractDistStage {

    private static final String SIZE_INFO = "SIZE_INFO";

    private transient CacheWrapper cacheWrapper;
    
    private transient MicrobenchmarkStressor[] microbenchmarkStressors;
    
    int localThreads;
    int range;
    int duration;
    int writeRatio;
    
    @Override
    public DistStageAck executeOnSlave() {
	DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
	this.cacheWrapper = slaveState.getCacheWrapper();
	if (cacheWrapper == null) {
	    log.info("Not running test on this slave as the wrapper hasn't been configured.");
	    return result;
	}

	log.info("Starting MicrobenchmarkBenchmarkStage: " + this.toString());
	
	microbenchmarkStressors = new MicrobenchmarkStressor[localThreads];
	
	for (int t = 0; t < microbenchmarkStressors.length; t++) {
	    microbenchmarkStressors[t] = new MicrobenchmarkStressor();
	    microbenchmarkStressors[t].setCacheWrapper(cacheWrapper);
	    microbenchmarkStressors[t].setRange(range);
	    microbenchmarkStressors[t].setWriteRatio(writeRatio);
	}
	
	try {
	    Thread[] workers = new Thread[microbenchmarkStressors.length];
	    for (int t = 0; t < workers.length; t++) {
		workers[t] = new Thread(microbenchmarkStressors[t]);
	    }
	    for (int t = 0; t < workers.length; t++) {
		workers[t].start();
	    }
	    try {
		Thread.sleep(duration);
	    } catch (InterruptedException e) {
	    }
	    for (int t = 0; t < workers.length; t++) {
		microbenchmarkStressors[t].setM_phase(MicrobenchmarkStressor.SHUTDOWN_PHASE);
	    }
	    
	    for (int t = 0; t < workers.length; t++) {
		workers[t].join();
	    }
	    Map<String, String> results = new LinkedHashMap<String, String>();
	    String sizeInfo = "size info: " + cacheWrapper.getInfo() +
		    ", clusterSize:" + super.getActiveSlaveCount() +
		    ", nodeIndex:" + super.getSlaveIndex() +
		    ", cacheSize: " + cacheWrapper.getCacheSize();
	    results.put(SIZE_INFO, sizeInfo);
	    long aborts = 0L;
	    double steps = 0.0;
	    for (int t = 0; t < workers.length; t++) {
		aborts += microbenchmarkStressors[t].getRestarts();
		steps += microbenchmarkStressors[t].getSteps();
	    }
	    results.put("TOTAL_THROUGHPUT", ((steps * 1000) / duration) + "");
	    results.put("TOTAL_RESTARTS", aborts + "");
	    log.info(sizeInfo);
	    result.setPayload(results);
	    return result;
	} catch (Exception e) {
	    log.warn("Exception while initializing the test", e);
	    result.setError(true);
	    result.setRemoteException(e);
	    return result;
	}
    }

    public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
	logDurationInfo(acks);
	boolean success = true;
	Map<Integer, Map<String, Object>> results = new HashMap<Integer, Map<String, Object>>();
	masterState.put("results", results);
	for (DistStageAck ack : acks) {
	    DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
	    if (wAck.isError()) {
		success = false;
		log.warn("Received error ack: " + wAck);
	    } else {
		if (log.isTraceEnabled())
		    log.trace(wAck);
	    }
	    Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
	    if (benchResult != null) {
		results.put(ack.getSlaveIndex(), benchResult);
		Object reqPerSes = benchResult.get("TOTAL_THROUGHPUT");
		if (reqPerSes == null) {
		    throw new IllegalStateException("This should be there!");
		}
		log.info("On slave " + ack.getSlaveIndex() + " had throughput " + Double.parseDouble(reqPerSes.toString()) + " ops/seconds");
		log.info("Received " +  benchResult.remove(SIZE_INFO));
	    } else {
		log.trace("No report received from slave: " + ack.getSlaveIndex());
	    }
	}
	return success;
    }
    
    public CacheWrapper getCacheWrapper() {
        return cacheWrapper;
    }

    public int getLocalThreads() {
        return localThreads;
    }

    public int getRange() {
        return range;
    }
    
    public int getDuration() {
        return duration;
    }

    public int getWriteRatio() {
        return writeRatio;
    }

    public void setCacheWrapper(CacheWrapper cacheWrapper) {
        this.cacheWrapper = cacheWrapper;
    }

    public void setLocalThreads(int localThreads) {
        this.localThreads = localThreads;
    }

    public void setRange(int range) {
        this.range = range;
    }
    
    public void setDuration(int duration) {
        this.duration = duration;
    }

    public void setWriteRatio(int writeRatio) {
        this.writeRatio = writeRatio;
    }
}
