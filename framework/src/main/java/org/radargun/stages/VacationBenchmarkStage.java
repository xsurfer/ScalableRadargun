package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.stages.stressors.stamp.vacation.VacationParameter;
import org.radargun.stages.stressors.stamp.vacation.VacationStressor;


public class VacationBenchmarkStage extends AbstractBenchmarkStage<VacationStressor, VacationParameter> {

   private static final String SIZE_INFO = "SIZE_INFO";

   //public static int THREADS;

   private int clients;

    /* number of threads per node */
   //private int localThreads;

   private int readOnly;

   /* number of operations per transaction */
   private int number;

   private int queries;

   /* the size of the tables */
   private int relations;

   /* the percentage of reservations */
   private int user;

//    private int time;


   @Override
   protected VacationParameter createStressorConfiguration() {
      log.trace("Creating VacationParameter...");

      VacationParameter parameters = new VacationParameter(
            cacheWrapper,
            simulationTimeSec,
            numOfThreads,
            getSlaveIndex(),
            backOffTime,
            retryOnAbort,
            statsSamplingInterval,
            number,
            user,
            queries,
            readOnly,
            relations
      );

      return parameters;
   }

   @Override
   public VacationStressor createStressor() {
      return new VacationStressor(cacheWrapper, this, system, getStressorParameters());
   }

   @Override
   @ManagedOperation(description = "Stop the current benchmark")
   public void stopBenchmark() {
      stressor.stopBenchmark();
   }

//    /* NEW EXECUTE by FABIO */
//    public DistStageAck executeOnSlave() {
//        DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress(), this.getClass().getName());
//        this.cacheWrapper = slaveState.getCacheWrapper();
//        if (cacheWrapper == null) {
//            log.info("Not running test on this slave as the wrapper hasn't been configured.");
//            return result;
//        }
//
//        log.info("Starting TpccBenchmarkStage: " + this.toString());
//
//        //trackNewKeys();
//
//        stressor = new VacationStressor(this.workloadGenerator);
//        stressor.setNodeIndex(getSlaveIndex());
//        stressor.setNumSlaves(getActiveSlaveCount());
//        stressor.setNumOfThreads(this.numOfThreads);
//        stressor.setSimulationTimeSec(this.simulationTimeSec);
//        stressor.setStatsSamplingInterval(statsSamplingInterval);
//        stressor.setBackOffTime(backOffTime);
//        stressor.setRetryOnAbort(retryOnAbort);
//        stressor.setRetrySameXact(retrySameXact);
//
//        /* Ad hoc for Vacation */
//
//        int percentUser = user;
//
//        stressor.setQueryPerTx(number);
//        stressor.setPercentUser(percentUser);
//        stressor.setQueryRange(queries);
//        stressor.setReadOnlyPerc(this.readOnly);
//        stressor.setCacheWrapper(cacheWrapper);
//        stressor.setRelations(relations);
//        /* end */
//
//        try {
//            Map<String, String> results = stressor.stress(cacheWrapper);
//            if( results != null ){
//                String sizeInfo = "size info: " + cacheWrapper.getInfo() +
//                        ", clusterSize:" + super.getActiveSlaveCount() +
//                        ", nodeIndex:" + super.getSlaveIndex() +
//                        ", cacheSize: " + cacheWrapper.getCacheSize();
//                log.info(sizeInfo);
//                results.put(SIZE_INFO, sizeInfo);
//            }
//            result.setPayload(results);
//            return result;
//        } catch (Exception e) {
//            log.warn("Exception while initializing the test", e);
//            result.setError(true);
//            result.setRemoteException(e);
//            return result;
//        }
//    }





    /* ********************* */
    /* *** GETTER/SETTER *** */
    /* ********************* */

   public CacheWrapper getCacheWrapper() {
      return cacheWrapper;
   }

   public void setCacheWrapper(CacheWrapper cacheWrapper) {
      this.cacheWrapper = cacheWrapper;
   }

   public int getClients() {
      return clients;
   }

   public void setClients(int clients) {
      this.clients = clients;
   }

//    public int getLocalThreads() { return localThreads; }
//    public void setLocalThreads(int localThreads) { this.localThreads = localThreads; }

   public int getNumber() {
      return number;
   }

   public void setNumber(int number) {
      this.number = number;
   }

   public int getQueries() {
      return queries;
   }

   public void setQueries(int queries) {
      this.queries = queries;
   }

   public int getRelations() {
      return relations;
   }

   public void setRelations(int relations) {
      this.relations = relations;
   }

//    public int getTime() { return time; }
//    public void setTime(int time) { this.time = time; }

   public int getUser() {
      return user;
   }

   public void setUser(int user) {
      this.user = user;
   }

   public static String getSizeInfo() {
      return SIZE_INFO;
   }

   public void setReadOnly(int ro) {
      this.readOnly = ro;
   }

}
