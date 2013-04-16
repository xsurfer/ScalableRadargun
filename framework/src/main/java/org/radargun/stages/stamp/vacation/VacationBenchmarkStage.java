package org.radargun.stages.stamp.vacation;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.portings.tpcc.transaction.AbstractTpccTransaction;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.DefaultDistStageAck;
import org.radargun.stressors.stamp.vacation.VacationStressor;
import org.radargun.state.MasterState;
import org.radargun.stressors.tpcc.TpccStressor;


public class VacationBenchmarkStage extends AbstractBenchmarkStage<VacationStressor> {

    private static final String SIZE_INFO = "SIZE_INFO";

    private transient CacheWrapper cacheWrapper;

    //public static int THREADS;

    private int clients;

    /* number of threads per node */
    private int localThreads;

    private int readOnly;

    /* number of operations per transaction */
    private int number;

    private int queries;

    /* the size of the tables */
    private int relations;

    /* the percentage of reservations */
    private int user;

//    private int time;


    /* NEW EXECUTE by FABIO */
    public DistStageAck executeOnSlave() {
        DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress(), this.getClass().getName());
        this.cacheWrapper = slaveState.getCacheWrapper();
        if (cacheWrapper == null) {
            log.info("Not running test on this slave as the wrapper hasn't been configured.");
            return result;
        }

        log.info("Starting TpccBenchmarkStage: " + this.toString());

        //trackNewKeys();

        stressor = new VacationStressor(this.workloadGenerator);
        stressor.setNodeIndex(getSlaveIndex());
        stressor.setNumSlaves(getActiveSlaveCount());
        stressor.setNumOfThreads(this.numOfThreads);
        stressor.setPerThreadSimulTime(this.perThreadSimulTime);
        stressor.setStatsSamplingInterval(statsSamplingInterval);
        stressor.setBackOffTime(backOffTime);
        stressor.setRetryOnAbort(retryOnAbort);
        stressor.setRetrySameXact(retrySameXact);

        /* Ad hoc for Vacation */
        int numQueryPerTransaction = number;
        int percentUser = user;

        stressor.setQueryPerTx(numQueryPerTransaction);
        stressor.setPercentUser(percentUser);
        stressor.setQueryRange(queries);
        stressor.setReadOnlyPerc(this.readOnly);
        stressor.setCacheWrapper(cacheWrapper);
        stressor.setRelations(relations);
        /* end */

        try {
            Map<String, String> results = stressor.stress(cacheWrapper);
            if( results != null ){
                String sizeInfo = "size info: " + cacheWrapper.getInfo() +
                        ", clusterSize:" + super.getActiveSlaveCount() +
                        ", nodeIndex:" + super.getSlaveIndex() +
                        ", cacheSize: " + cacheWrapper.getCacheSize();
                log.info(sizeInfo);
                results.put(SIZE_INFO, sizeInfo);
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

    @Override
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



    /* ********************* */
    /* *** GETTER/SETTER *** */
    /* ********************* */

    public CacheWrapper getCacheWrapper() { return cacheWrapper; }
    public void setCacheWrapper(CacheWrapper cacheWrapper) { this.cacheWrapper = cacheWrapper; }

    public int getClients() { return clients; }
    public void setClients(int clients) { this.clients = clients; }

    public int getLocalThreads() { return localThreads; }
    public void setLocalThreads(int localThreads) { this.localThreads = localThreads; }

    public int getNumber() { return number; }
    public void setNumber(int number) { this.number = number; }

    public int getQueries() { return queries; }
    public void setQueries(int queries) { this.queries = queries; }

    public int getRelations() { return relations; }
    public void setRelations(int relations) { this.relations = relations; }

//    public int getTime() { return time; }
//    public void setTime(int time) { this.time = time; }

    public int getUser() { return user; }
    public void setUser(int user) { this.user = user; }

    public static String getSizeInfo() { return SIZE_INFO; }

    public void setReadOnly(int ro) { this.readOnly = ro; }

}
