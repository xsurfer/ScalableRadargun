package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.stamp.vacation.VacationStressor;
import org.radargun.state.MasterState;
import org.radargun.ycsb.YCSB;
import org.radargun.ycsb.YCSBStressor;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class YCSBBenchmarkStage extends AbstractDistStage {

   private static final String SIZE_INFO = "SIZE_INFO";

   private transient CacheWrapper cacheWrapper;

   private transient YCSBStressor[] ycsbStressors;

   private int multiplereadcount;
   private int recordcount;
   private int executiontime;
   private int threads;
   private int readonly;

   @Override
   public DistStageAck executeOnSlave() {
      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
      this.cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         log.info("Not running test on this slave as the wrapper hasn't been configured.");
         return result;
      }

      log.info("Starting YCSBBenchmarkStage: " + this.toString());

      YCSB.init(this.readonly, recordcount);
      ycsbStressors = new YCSBStressor[threads];

      for (int t = 0; t < ycsbStressors.length; t++) {

         ycsbStressors[t] = new YCSBStressor();
         ycsbStressors[t].setCacheWrapper(cacheWrapper);
         ycsbStressors[t].setRecordCount(this.recordcount);
         ycsbStressors[t].setMultiplereadcount(this.multiplereadcount);
      }

      try {
         Thread[] workers = new Thread[ycsbStressors.length];
         for (int t = 0; t < workers.length; t++) {
            workers[t] = new Thread(ycsbStressors[t]);
         }
         for (int t = 0; t < workers.length; t++) {
            workers[t].start();
         }
         try {
            Thread.sleep(executiontime);
         } catch (InterruptedException e) {
         }
         for (int t = 0; t < workers.length; t++) {
            ycsbStressors[t].setPhase(VacationStressor.SHUTDOWN_PHASE);
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
         long throughput = 0L;
         for (int t = 0; t < workers.length; t++) {
            aborts += ycsbStressors[t].getRestarts();
            throughput += ycsbStressors[t].getThroughput();
         }
         results.put("THROUGHPUT", (((throughput + 0.0) * 1000) / executiontime) + "");
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
            Object reqPerSes = benchResult.get("THROUGHPUT");
            if (reqPerSes == null) {
               throw new IllegalStateException("This should be there!");
            }
            log.info("On slave " + ack.getSlaveIndex() + " it took " + (Double.parseDouble(reqPerSes.toString()) / 1000.0) + " seconds");
            log.info("Received " + benchResult.remove(SIZE_INFO));
         } else {
            log.trace("No report received from slave: " + ack.getSlaveIndex());
         }
      }
      return success;
   }

   public CacheWrapper getCacheWrapper() {
      return cacheWrapper;
   }

   public void setCacheWrapper(CacheWrapper cacheWrapper) {
      this.cacheWrapper = cacheWrapper;
   }

   public int getMultiplereadcount() {
      return multiplereadcount;
   }

   public void setMultiplereadcount(int multiplereadcount) {
      this.multiplereadcount = multiplereadcount;
   }

   public int getRecordcount() {
      return recordcount;
   }

   public void setRecordcount(int recordcount) {
      this.recordcount = recordcount;
   }

   public int getExecutiontime() {
      return executiontime;
   }

   public void setExecutiontime(int executiontime) {
      this.executiontime = executiontime;
   }

   public int getThreads() {
      return threads;
   }

   public void setThreads(int threads) {
      this.threads = threads;
   }

   public int getReadonly() {
      return readonly;
   }

   public void setReadonly(int readonly) {
      this.readonly = readonly;
   }
}
