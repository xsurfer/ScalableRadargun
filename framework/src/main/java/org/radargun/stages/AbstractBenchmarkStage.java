package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.SlaveSocketChannel;
import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedAttribute;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.Parameters;
import org.radargun.stages.stressors.systems.IProducerSystem;
import org.radargun.stages.stressors.systems.OpenSystem;
import org.radargun.stages.stressors.systems.System;
import org.radargun.stages.synthetic.XACT_RETRY;
import org.radargun.state.MasterState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Double.parseDouble;
import static org.radargun.utils.Utils.numberFormat;

/**
 * Created by: Fabio Perfetti E-mail: perfabio87@gmail.com Date: 3/23/13
 */

@MBean(objectName = "AbstractBenchmark", description = "Abstract benchmark stage")
public abstract class AbstractBenchmarkStage<T extends AbstractBenchmarkStressor, S extends Parameters> extends AbstractDistStage {

    /* ***************** */
    /* ** ATTRIBUTES *** */
    /* ***************** */

   private static final String SCRIPT_LAUNCH = "_script_launch_";

   private static final String SCRIPT_PATH = "~/pedroGun/beforeBenchmark.sh";

   /**
    * the number of threads that will work on this slave
    */
   protected int numOfThreads = 10;

   /**
    * total time (in seconds) of simulation for each stressor thread
    */
   protected long simulationTimeSec = 180L;

   /**
    * specify the interval period (in milliseconds) of the memory and cpu usage is collected
    */
   protected long statsSamplingInterval = 0;

   /**
    * the system type
    */
   protected System system;

   /* istanza di AbstractBenchmarkStressor */
   protected transient T stressor;

   protected transient CacheWrapper cacheWrapper;

   /**
    * If true, a transaction t is regenerated until it commits, unless it throws a "NotSuchElementException" In this
    * case, the transaction is aborted for good.
    */
   protected XACT_RETRY retryOnAbort = XACT_RETRY.NO_RETRY;

   protected boolean retrySameXact = false;

   /**
    * Specifies the msec a transaction spends in backoff after aborting
    */
   protected long backOffTime = 0;

   protected static final String SIZE_INFO = "SIZE_INFO";

   /* if true, new keys are tracked so that they can be erased in the end of the stage */
   protected boolean trackNewKeys = false;

   protected boolean perThreadTrackNewKeys = false;

   private S parameters;


    /* ****************** */
    /* ** TO OVERRIDE *** */
    /* ****************** */

   protected abstract S createStressorConfiguration();

   protected abstract T createStressor();



    /* *************** */
    /* *** METHODS *** */
    /* *************** */


   protected S getStressorParameters() {

      if (parameters != null) {
         log.trace("Parameters already created");
         return parameters;
      }

      S parameters = createStressorConfiguration();

      return parameters;
   }


   public void initOnMaster(MasterState masterState, int slaveIndex) {
      super.initOnMaster(masterState, slaveIndex);
      Boolean started = (Boolean) masterState.get(SCRIPT_LAUNCH);
      if (started == null || !started) {
         masterState.put(SCRIPT_LAUNCH, startScript());
      }
   }


   @Override
   public DistStageAck executeOnSlave() {

      this.cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         throw new IllegalStateException("Not running test on this slave as the wrapper hasn't been configured");
      }

      log.info("Starting BenchmarkStage: " + this.toString());

      //trackNewKeys();

      stressor = createStressor();

      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress(), this.getClass().getName());
      try {
         Map<String, String> results;
         log.info("Slave info: cacheSize " + cacheWrapper.getCacheSize());
         results = stressor.stress();

         if (results != null) {

            String sizeInfo = "size info: " + cacheWrapper.getInfo() +
                  ", clusterSize:" + super.getActiveSlaveCount() +
                  ", nodeIndex:" + super.getSlaveIndex() +
                  ", cacheSize: " + cacheWrapper.getCacheSize();
            log.info(sizeInfo);
            results.put(SIZE_INFO, sizeInfo);
            if (Boolean.parseBoolean(results.get("STOPPED"))) {
               //log.info("Cache has been torn down!");
               //cacheWrapper.tearDown();
               log.info("Don't tear down the cache, the slave should die in a while, so it will tears it down!");
            }
         }
         result.setPayload(results);
         return result;

      } catch (Exception e) {
         log.warn("Exception while initializing the test", e);
         result.setError(true);
         result.setRemoteException(e);
         return result;
      }
   }


   //If in subsequent runs I want to sue different methods, I have to ensure that only one is active
   private void trackNewKeys() {
      if (trackNewKeys && perThreadTrackNewKeys)
         throw new IllegalArgumentException("trackNewKeys and perThreadTrackNewKeys should be mutually exclusive (at least for now)");
      this.cacheWrapper.setPerThreadTrackNewKeys(false);
      this.cacheWrapper.setTrackNewKeys(false);
      cacheWrapper.setTrackNewKeys(trackNewKeys);
      cacheWrapper.setPerThreadTrackNewKeys(perThreadTrackNewKeys);
   }


   public void updateTimes(final long initTsCurrentStage) {
      log.info("Updating simulationTimeSec");

      long totalSimulTime = this.getSimulationTimeSec();
      log.info("totalSimulTime: " + totalSimulTime);


      log.info("currentMainStageInitTs: " + initTsCurrentStage);

      long toExecuteInitTs = java.lang.System.currentTimeMillis();
      log.info("toExecuteInitTs: " + toExecuteInitTs);

      long elapsedTimeFromBeginning = toExecuteInitTs - initTsCurrentStage;
      log.info("elapsedTimeFromBeginning: " + elapsedTimeFromBeginning);

      long secondToExecute = totalSimulTime - (elapsedTimeFromBeginning / 1000);
      log.info("secondToExecute: " + secondToExecute);

      if (secondToExecute < 0) {
         secondToExecute = 0;
      }

      log.info("This stage will execute for: " + secondToExecute);
      this.setSimulationTimeSec(secondToExecute);

      log.info("Updating initTime Workload Generator");
      if (system.getType().equals(System.SystemType.OPEN)) {

         long granularity = ((OpenSystem) system).getWorkloadGenerator().getGranularity();
         log.info("granularity: " + granularity);

         double numTicks = (double) elapsedTimeFromBeginning / (double) granularity;
         log.info("numTicks: " + numTicks);

         int intNumTicks = (int) numTicks;
         log.info("intNumTicks: " + intNumTicks);

         double decimalNumTicks = numTicks - (double) intNumTicks;
         log.info("decimalNumTicks: " + decimalNumTicks);

         long firstTick = granularity - (long) (decimalNumTicks * granularity);
         log.info("firstTick: " + firstTick);

         double originalInitTime = ((OpenSystem) system).getWorkloadGenerator().getInitTime();
         log.info("originalInitTime: " + originalInitTime);

         long initTime = (long) originalInitTime + (intNumTicks * granularity) / 1000;
         log.info("initTime: " + initTime);

         ((OpenSystem) system).getWorkloadGenerator().setInitTime(initTime);
         ((OpenSystem) system).getWorkloadGenerator().setFirstGranularity(firstTick);
      }
   }


   /**
    * This method iterates acks list looking for nodes stopped by JMX.<br/>
    *
    * @param acks   Acks from previous stage
    * @param slaves All the slaves actually running the test
    * @return List of slaveIndex stopped by JMX
    */
   public List<Integer> sizeForNextStage(List<DistStageAck> acks, List<SlaveSocketChannel> slaves) {
      List<Integer> ret = new ArrayList<Integer>();

      if (acks.size() != slaves.size())
         throw new IllegalStateException("Number of acks and number of slaves MUST be ugual");

      for (DistStageAck ack : acks) {
         DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
         Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
         if (benchResult != null) {
            Object stoppedByJmxObj = benchResult.get("STOPPED");
            if (stoppedByJmxObj == null) {
               throw new IllegalStateException("STOPPED should be there!");
            }
            boolean stoppedByJmx = Boolean.parseBoolean(stoppedByJmxObj.toString());
            if (stoppedByJmx) {
               log.info("Slave " + ack.getSlaveIndex() + " has been stopped via JMX, I don't use it anymore");
               ret.add(new Integer(ack.getSlaveIndex()));
            }
         } else {
            log.warn("No report received from slave: " + ack.getSlaveIndex());
         }
      }

      log.info("Next stage will start with: " + (slaves.size() - ret.size()) + " slaves");
      return ret;
   }

   public AbstractBenchmarkStage clone() {
      AbstractBenchmarkStage clone = (AbstractBenchmarkStage) super.clone();
      log.info("cloning AbstractBenchmarkStage");

      clone.system = system.clone();
//        if( clone.system instanceof OpenSystem ){
//            log.info("Cloning System");
//            double initTime = ((OpenSystem) system).getWorkloadGenerator().getInitTime();
//            ((OpenSystem) clone.system).getWorkloadGenerator().setInitTime(initTime);
//        }
//        clone.workloadGenerator = workloadGenerator.clone();

      return clone;
   }

   @Override
   public final boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
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
            Object reqPerSes = benchResult.get("REQ_PER_SEC");
            if (reqPerSes == null) {
               throw new IllegalStateException("This should be there!");
            }
            log.info("On slave " + ack.getSlaveIndex() + " we had " + numberFormat(parseDouble(reqPerSes.toString())) + " requests per second");
            log.info("Received " + benchResult.remove(SIZE_INFO));
         } else {
            log.trace("No report received from slave: " + ack.getSlaveIndex());
         }
      }
      return success;
   }

   private Boolean startScript() {
      try {
         Runtime.getRuntime().exec(SCRIPT_PATH);
         log.info("Script " + SCRIPT_PATH + " started successfully");
         return Boolean.TRUE;
      } catch (Exception e) {
         log.warn("Error starting script " + SCRIPT_PATH + ". " + e.getMessage());
         return Boolean.FALSE;
      }
   }


    /* ******************************* */
    /* *** MANAGED METHODS VIA JMX *** */
    /* ******************************* */

   @ManagedAttribute(description = "Returns the number of threads created", writable = false)
   public final int getNumOfThreads() {
      return stressor.getNumberOfThreads();
   }

   @ManagedAttribute(description = "Returns the number of threads actually running", writable = false)
   public final int getNumberOfActiveThreads() {
      return stressor.getNumberOfActiveThreads();
   }

   @ManagedOperation(description = "Change the number of threads running, creating more threads if needed")
   public final void setNumberOfActiveThreads(int numberOfActiveThreads) {
      stressor.setNumberOfRunningThreads(numberOfActiveThreads);
   }


   @ManagedOperation(description = "Retrieve current arrivalRate")
   public int arrivalRate() {
      if (system.getType().equals(System.SystemType.OPEN)) {
         return ((OpenSystem) system).getWorkloadGenerator().arrivalRate();
      }
      return -1;
   }

   @ManagedOperation(description = "Retrieve current queue size")
   public int queueSize() {

      if (system instanceof IProducerSystem) {
         return stressor.queueSize();
      } else {
         return -1;
      }
   }

   @ManagedOperation(description = "Stop the current benchmark")
   public void stopBenchmark() {
      log.info("****************************************");
      log.info("************STOPPING BY JMX*************");
      log.info("stage: " + getId());
      log.info("****************************************");
      stressor.stopBenchmark();
   }

   @ManagedOperation(description = "Notify the slave that a new slave is ready to execute txs")
   public void changeNumNodes() {
      stressor.changeNumberNodes();
   }


    /* ***********************/
    /* *** GETTER/SETTER *** */
    /* ********************* */

   public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }

   public void setBackOffTime(long backOffTime) {
      this.backOffTime = backOffTime;
   }

   public void setRetryOnAbort(String retryOnAbort) {
      this.retryOnAbort = XACT_RETRY.valueOf(retryOnAbort);
   }

   public void setStatsSamplingInterval(long statsSamplingInterval) {
      this.statsSamplingInterval = statsSamplingInterval;
   }

   public void setSysyemType(System val) {
      this.system = val;
   }

   public System getSysyemType() {
      return this.system;
   }

   public long getSimulationTimeSec() {
      return this.simulationTimeSec;
   }

   public void setSimulationTimeSec(long simulationTimeSec) {
      this.simulationTimeSec = simulationTimeSec;
   }

   //public long getInitTimeStamp() { return this.initTimeStamp; }
   //public void setInitTimeStamp() { this.initTimeStamp = System.currentTimeMillis(); log.info("SETTING initTimeStamp to: " + initTimeStamp); }

   //public AbstractBenchmarkStressor getStressor(){ return stressor; }

}
