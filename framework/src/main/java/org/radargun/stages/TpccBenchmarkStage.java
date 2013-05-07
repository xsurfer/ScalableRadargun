package org.radargun.stages;

import org.radargun.jmx.annotations.MBean;

import org.radargun.portings.tpcc.transaction.AbstractTpccTransaction;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.tpcc.TpccStressor;
import org.radargun.stages.stressors.tpcc.TpccStressorParameter;


/**
 * Simulate the activities found in complex OLTP application environments.
 * Execute the TPC-C Benchmark.
 * <pre>
 * Params:
 *       - numOfThreads : the number of stressor threads that will work on each slave.
 *       - updateTimes : total time (in seconds) of simulation for each stressor thread.
 *       - arrivalRate : if the value is greater than 0.0, the "open system" mode is active and the parameter represents the arrival rate (in transactions per second) of a job (a transaction to be executed) to the system; otherwise the "closed system" mode is active: this means that each thread generates and executes a new transaction in an iteration as soon as it has completed the previous iteration.
 *       - paymentWeight : percentage of Payment transactions.
 *       - orderStatusWeight : percentage of Order Status transactions.
 * </pre>
 *
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
@MBean(objectName = "TpccBenchmark", description = "TPC-C benchmark stage that generates the TPC-C workload")
public class TpccBenchmarkStage extends AbstractBenchmarkStage<TpccStressor, TpccStressorParameter> {


    /**
     * percentage of Payment transactions
     */
    private int paymentWeight = 45;

    /**
     * percentage of Order Status transactions
     */
    private int orderStatusWeight = 5;

    /**
     * if true, each node will pick a warehouse and all transactions will work over that warehouse. The warehouses are
     * picked by order, i.e., slave 0 gets warehouse 1,N+1, 2N+1,[...]; ... slave N-1 gets warehouse N, 2N, [...].
     */
    private boolean accessSameWarehouse = false;

    /**
     * specify the min and the max number of items created by a New Order Transaction.
     * format: min,max
     */
    private String numberOfItemsInterval = null;

    /*
    If true, notSuchElement exception is not thrown in  transactions if "choose by last name"
     */
    private boolean avoidMiss = true;



    /* ****************** */
    /* *** OVERRIDING *** */
    /* ****************** */

    @Override
    protected TpccStressorParameter createStressorConfiguration(){
        TpccStressorParameter parameters = new TpccStressorParameter();
        parameters.setPaymentWeight(paymentWeight);
        parameters.setOrderStatusWeight(orderStatusWeight);
        parameters.setAccessSameWarehouse(accessSameWarehouse);
        parameters.setNumberOfItemsInterval(numberOfItemsInterval);

        AbstractTpccTransaction.setAvoidNotFoundExceptions(avoidMiss);
        return parameters;
    }

    @Override
    public AbstractBenchmarkStressor createStressor() {
        return new TpccStressor(cacheWrapper, this, system, getStressorParameters());
    }


//    @Override
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
//        trackNewKeys();
//
//        stressor = new TpccStressor(this.workloadGenerator);
//        stressor.setNodeIndex(getSlaveIndex());
//        stressor.setNumSlaves(getActiveSlaveCount());
//        stressor.setNumOfThreads(this.numOfThreads);
//        stressor.setPerThreadSimulTime(this.perThreadSimulTime);
//        stressor.setStatsSamplingInterval(statsSamplingInterval);
//        stressor.setBackOffTime(backOffTime);
//        stressor.setRetryOnAbort(retryOnAbort);
//        stressor.setRetrySameXact(retrySameXact);
//        stressor.setPaymentWeight(this.paymentWeight);
//        stressor.setOrderStatusWeight(this.orderStatusWeight);
//        stressor.setAccessSameWarehouse(accessSameWarehouse);
//        stressor.setNumberOfItemsInterval(numberOfItemsInterval);
//
//
//        AbstractTpccTransaction.setAvoidNotFoundExceptions(this.avoidMiss);
//
//        try {
//            Map<String, String> results = stressor.stress(cacheWrapper);
//            if (results != null) {
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

    @Override
    public String toString() {
        return "TpccBenchmarkStage {" +
                "numOfThreads=" + numOfThreads +
                ", updateTimes=" + perThreadSimulTime +
                ", paymentWeight=" + paymentWeight +
                ", orderStatusWeight=" + orderStatusWeight +
                ", accessSameWarehouse=" + accessSameWarehouse +
                ", numberOfItemsInterval=" + numberOfItemsInterval +
                ", statsSamplingInterval=" + statsSamplingInterval +
                ", cacheWrapper=" + cacheWrapper +
                ", " + super.toString();
    }



    /* *************** */
    /* *** METHODS *** */
    /* *************** */






    /* ***********************/
    /* *** GETTER/SETTER *** */
    /* ********************* */


    public void setPaymentWeight(int paymentWeight) {
        this.paymentWeight = paymentWeight;
    }

    public void setOrderStatusWeight(int orderStatusWeight) {
        this.orderStatusWeight = orderStatusWeight;
    }

    public void setAccessSameWarehouse(boolean accessSameWarehouse) {
        this.accessSameWarehouse = accessSameWarehouse;
    }

    public void setNumberOfItemsInterval(String numberOfItemsInterval) {
        this.numberOfItemsInterval = numberOfItemsInterval;
    }

    public void setAvoidMiss(boolean avoidMiss) {
        this.avoidMiss = avoidMiss;
    }

    public void setTrackNewKeys(boolean trackNewKeys) {
        this.trackNewKeys = trackNewKeys;
    }

    public void setPerThreadTrackNewKeys(boolean trackNewKeys) {
        this.perThreadTrackNewKeys = trackNewKeys;
    }


//   @ManagedOperation(description = "Change the workload to decrease contention between transactions")
//   public void lowContention(int payment, int order) {
//      tpccStressor.lowContention(payment, order);
//   }
//
//   @ManagedOperation(description = "Change the workload to increase contention between transactions")
//   public void highContention(int payment, int order) {
//      tpccStressor.highContention(payment, order);
//   }
//
//   @ManagedOperation(description = "Change the workload to random select the warehouse to work with")
//   public void randomContention(int payment, int order) {
//      tpccStressor.randomContention(payment, order);
//   }

//    @ManagedAttribute(description = "Returns the expected write percentage workload", writable = false)
//    public final double getExpectedWritePercentage() {
//        return stressor.getExpectedWritePercentage();
//    }
//
//    @ManagedAttribute(description = "Returns the Payment transaction type percentage", writable = false)
//    public final int getPaymentWeight() {
//        return stressor.getPaymentWeight();
//    }
//
//    @ManagedAttribute(description = "Returns the Order Status transaction type percentage", writable = false)
//    public final int getOrderStatusWeight() {
//        return stressor.getOrderStatusWeight();
//    }

    public TpccBenchmarkStage clone() {
        return (TpccBenchmarkStage) super.clone();
    }

}
