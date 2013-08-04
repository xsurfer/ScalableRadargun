package org.radargun.stages.stressors.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.portings.tpcc.TpccTerminal;
import org.radargun.portings.tpcc.TpccTools;
import org.radargun.portings.tpcc.TpccTxFactory;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.producer.Producer;
import org.radargun.stages.stressors.systems.SystemType;
import org.radargun.stages.stressors.tpcc.consumer.TpccConsumer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
* On multiple threads executes implementations of TPC-C Transaction Profiles against the CacheWrapper, and returns the
* result as a Map.
*
* @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
* @author Pedro Ruivo
* @author Fabio Perfetti (refactored)
*/

public class TpccStressor extends AbstractBenchmarkStressor<TpccParameters, TpccConsumer, Producer, TpccTxFactory> {


    private static Log log = LogFactory.getLog(TpccStressor.class);

    private List<TpccTerminal> terminals = new ArrayList<TpccTerminal>();

    private final List<Integer> listLocalWarehouses = new LinkedList<Integer>();

    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public TpccStressor(CacheWrapper cacheWrapper, AbstractBenchmarkStage benchmarkStage, SystemType system, TpccParameters parameters) {
        super(cacheWrapper, benchmarkStage, system, parameters);
    }

    /* ****************** */
    /* *** OVERRIDING *** */
    /* ****************** */

    public TpccStats createStatsContainer(){
        return new TpccStats();
    }

    protected void extractExtraStats(TpccStats totalStats, TpccStats singleStats){

        totalStats.inc(TpccStats.NEW_ORDER_DURATION, singleStats.get(TpccStats.NEW_ORDER_DURATION)); // in nanosec
        totalStats.inc(TpccStats.PAYMENT_DURATION, singleStats.get(TpccStats.PAYMENT_DURATION)); // in nanosec
        totalStats.inc(TpccStats.NEW_ORDER_SERVICE_TIME, singleStats.get(TpccStats.NEW_ORDER_SERVICE_TIME));
        totalStats.inc(TpccStats.PAYMENT_SERVICE_TIME, singleStats.get(TpccStats.PAYMENT_SERVICE_TIME));
        totalStats.inc(TpccStats.NEW_ORDER, singleStats.get(TpccStats.NEW_ORDER));
        totalStats.inc(TpccStats.PAYMENT, singleStats.get(TpccStats.PAYMENT));
        totalStats.inc(TpccStats.NR_NEW_ORDER_FAILURES, singleStats.get(TpccStats.NR_NEW_ORDER_FAILURES));
        totalStats.inc(TpccStats.NR_PAYMENT_FAILURES, singleStats.get(TpccStats.NR_PAYMENT_FAILURES));
        totalStats.inc(TpccStats.NEW_ORDER_IN_QUEUE_TIME, singleStats.get(TpccStats.NEW_ORDER_IN_QUEUE_TIME));
        totalStats.inc(TpccStats.PAYMENT_IN_QUEUE_TIME, singleStats.get(TpccStats.PAYMENT_IN_QUEUE_TIME));
        totalStats.inc(TpccStats.NUM_NEW_ORDER_DEQUEUED, singleStats.get(TpccStats.NUM_NEW_ORDER_DEQUEUED));
        totalStats.inc(TpccStats.NUM_PAYMENT_DEQUEUED, singleStats.get(TpccStats.NUM_PAYMENT_DEQUEUED));
    }

    //@Override
    protected void fillMapWithExtraStats(TpccStats totalStats, Map<String, String> results){

        /* 1) Converting from nanoseconds to milliseconds && filling the stats obj */
        totalStats.put(TpccStats.NEW_ORDER_DURATION, totalStats.get(TpccStats.NEW_ORDER_DURATION) / 1000); //nanosec to microsec
        totalStats.put(TpccStats.PAYMENT_DURATION, totalStats.get(TpccStats.PAYMENT_DURATION) / 1000); //nanosec to microsec
        totalStats.put(TpccStats.NEW_ORDER_SERVICE_TIME, totalStats.get(TpccStats.NEW_ORDER_SERVICE_TIME) / 1000); //nanosec to microsec
        totalStats.put(TpccStats.PAYMENT_SERVICE_TIME, totalStats.get(TpccStats.PAYMENT_SERVICE_TIME) / 1000); //nanosec to microsec
        totalStats.put(TpccStats.NEW_ORDER_IN_QUEUE_TIME, totalStats.get(TpccStats.NEW_ORDER_IN_QUEUE_TIME) / 1000); //nanosec to microsec
        totalStats.put(TpccStats.PAYMENT_IN_QUEUE_TIME, totalStats.get(TpccStats.PAYMENT_IN_QUEUE_TIME) / 1000); //nanosec to microsec

        /* 2) Filling the map */
        results.put("NEW_ORDER_PER_SEC", str( totalStats.evalNewOrderPerSec() ) );
        results.put("PAYMENT_PER_SEC", str( totalStats.evalPaymentPerSec() ) );
        results.put("NEW_ORDER_COUNT", str( totalStats.get(TpccStats.NEW_ORDER) ));
        results.put("PAYMENT_COUNT", str( totalStats.get(TpccStats.PAYMENT) ));
        results.put("NEW_ORDER_FAILURES", str( totalStats.get(TpccStats.NR_NEW_ORDER_FAILURES) ));
        results.put("PAYMENT_FAILURES", str( totalStats.get(TpccStats.NR_PAYMENT_FAILURES) ));
        results.put("AVG_NEW_ORDER_SERVICE_TIME (usec)", str( totalStats.evalAvgNewOrderServiceTime() ));
        results.put("AVG_PAYMENT_SERVICE_TIME (usec)", str( totalStats.evalAvgPaymentServiceTime() ));
        results.put("AVG_NEW_ORDER_INQUEUE_TIME (usec)", str( totalStats.evalAvgNewOrderInQueueTime() ));
        results.put("AVG_PAYMENT_INQUEUE_TIME (usec)", str( totalStats.evalAvgPaymentInQueueTime() ));
        results.put("TEST_ID", this.testIdString(parameters.getPaymentWeight(), parameters.getOrderStatusWeight(), parameters.getNumOfThreads()));

    }

    @Override
    protected void initialization(){
        updateNumberOfItemsInterval();
        initializeToolsParameters();
        calculateLocalWarehouses();
    }

//    @Override
//    public int nextTransaction(int threadIndex) {
//        TpccTerminal terminal = new TpccTerminal(parameters.getPaymentWeight(), parameters.getOrderStatusWeight(), parameters.getNodeIndex(), 0);
//
//        return terminal.chooseTransactionType(
//                cacheWrapper.isPassiveReplication(),
//                cacheWrapper.isTheMaster()
//        );
//
//        /*
//        return new RequestType( System.nanoTime(), terminal.chooseTransactionType(
//                                                                                  cacheWrapper.isPassiveReplication(),
//                                                                                  cacheWrapper.isTheMaster()
//                                                                                ) );
//        */
//    }
//
//    @Override
//    public ITransaction generateTransaction(RequestType type, int threadIndex) {
//        TpccConsumer consumer = this.consumers.get(threadIndex);
//        ITransaction transaction = consumer.getTerminal().createTransaction(type.getTransactionType(), threadIndex);
//        return transaction;
//    }
//
//    @Override
//    public ITransaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId) {
//        TpccConsumer consumer = this.consumers.get(threadId);
//        ITransaction transaction = consumer.getTerminal().choiceTransaction(cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster(), threadId);
//        log.info("Closed system: starting a brand new transaction of type " + transaction.getType());
//        return transaction;
//    }

    @Override
    protected TpccConsumer createConsumer(int threadIndex) {
        int localWarehouse = getWarehouseForThread(threadIndex);




        return new TpccConsumer(localWarehouse, threadIndex, cacheWrapper, system, benchmarkStage, this, parameters, createTransactionFactory(threadIndex) );
    }


    @Override
    protected void validateTransactionsWeight() {
        int sum = parameters.getOrderStatusWeight() + parameters.getPaymentWeight();
        if (sum < 0 || sum > 100) {
            throw new IllegalArgumentException("The sum of the transactions weights must be higher or equals than zero " +
                    "and less or equals than one hundred");
        }
    }

    @Override
    protected double getWriteWeight() {
        double writeWeight = Math.max(100 - parameters.getOrderStatusWeight(), parameters.getPaymentWeight()) / 100D;
        return writeWeight;
    }

    @Override
    protected double getReadWeight() {
        double readWeight = parameters.getOrderStatusWeight() / 100D;
        return readWeight;
    }

    @Override
    protected TpccTxFactory createTransactionFactory(int threadIndex) {
        return new TpccTxFactory(parameters, threadIndex);
    }




    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    private void updateNumberOfItemsInterval() {
        if (parameters.getNumberOfItemsInterval() == null) {
            return;
        }
        String[] split = parameters.getNumberOfItemsInterval().split(",");

        if (split.length != 2) {
            log.info("Cannot update the min and max values for the number of items in new order transactions. " +
                    "Using the default values");
            return;
        }

        try {
            TpccTools.NUMBER_OF_ITEMS_INTERVAL[0] = Integer.parseInt(split[0]);
        } catch (NumberFormatException nfe) {
            log.warn("Min value is not a number. " + nfe.getLocalizedMessage());
        }

        try {
            TpccTools.NUMBER_OF_ITEMS_INTERVAL[1] = Integer.parseInt(split[1]);
        } catch (NumberFormatException nfe) {
            log.warn("Max value is not a number. " + nfe.getLocalizedMessage());
        }
    }

    private void initializeToolsParameters() {
        try {
            TpccTools.C_C_LAST = (Long) cacheWrapper.get(null, "C_C_LAST");

            TpccTools.C_C_ID = (Long) cacheWrapper.get(null, "C_C_ID");

            TpccTools.C_OL_I_ID = (Long) cacheWrapper.get(null, "C_OL_ID");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /* ********************* */
    /* *** GETTER/SETTER *** */
    /* ********************* */

//    public void setPaymentWeight(int paymentWeight) {
//        this.paymentWeight = paymentWeight;
//    }
//
//    public void setOrderStatusWeight(int orderStatusWeight) {
//        this.orderStatusWeight = orderStatusWeight;
//    }
//
//    public void setAccessSameWarehouse(boolean accessSameWarehouse) {
//        this.accessSameWarehouse = accessSameWarehouse;
//    }
//
//    public void setNumberOfItemsInterval(String numberOfItemsInterval) {
//        this.numberOfItemsInterval = numberOfItemsInterval;
//    }



    @Override
    public String toString() {
        return "TpccStressor{" +
                "updateTimes=" + parameters.getSimulationTimeSec() +
                ", paymentWeight=" + parameters.getPaymentWeight() +
                ", orderStatusWeight=" + parameters.getOrderStatusWeight() +
                ", accessSameWarehouse=" + parameters.isAccessSameWarehouse() +
                ", numSlaves=" + parameters.getNumSlaves() +
                ", nodeIndex=" + parameters.getNodeIndex() +
                ", numOfThreads=" + parameters.getNumOfThreads() +
                ", numberOfItemsInterval=" + parameters.getNumberOfItemsInterval() +
                ", statsSamplingInterval=" + parameters.getStatsSamplingInterval() +
                '}';
    }


    private void calculateLocalWarehouses() {
        if (parameters.isAccessSameWarehouse()) {
            TpccTools.selectLocalWarehouse(parameters.getNumSlaves(), parameters.getNodeIndex(), listLocalWarehouses);
            if (log.isDebugEnabled()) {
                log.debug("Find the local warehouses. Number of Warehouses=" + TpccTools.NB_WAREHOUSES + ", number of slaves=" +
                        parameters.getNumSlaves() + ", node index=" + parameters.getNodeIndex() + ".Local warehouses are " + listLocalWarehouses);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Local warehouses are disabled. Choose a random warehouse in each transaction");
            }
        }
    }


    private int getWarehouseForThread(int threadIdx) {
        return listLocalWarehouses.isEmpty() ? -1 : listLocalWarehouses.get(threadIdx % listLocalWarehouses.size());
    }

    public synchronized final double getExpectedWritePercentage() {
        return 1.0 - (parameters.getOrderStatusWeight() / 100.0);
    }

    public synchronized final int getPaymentWeight() {
        return parameters.getPaymentWeight();
    }

    public synchronized final int getOrderStatusWeight() {
        return parameters.getOrderStatusWeight();
    }



    public synchronized final void highContention(int payment, int order) {
        if (!running.get()) {
            return;
        }
        parameters.setPaymentWeight(payment);
        parameters.setOrderStatusWeight(order);

        log.info("Change to high contention workload:");
        for (TpccConsumer consumer : consumers) {
            consumer.getTerminal().change(1, parameters.getPaymentWeight(), parameters.getOrderStatusWeight());
            //log.info(consumer.getName() + " terminal is " + consumer.getFactory());
            log.info("randomContention for consumer: " + consumer.getThreadIndex());
        }
    }

    public synchronized final void lowContention(int payment, int order) {
        if (!running.get()) {
            return;
        }
        if (listLocalWarehouses.isEmpty()) {
            TpccTools.selectLocalWarehouse(parameters.getNumSlaves(), parameters.getNodeIndex(), listLocalWarehouses);
        }

        parameters.setPaymentWeight(payment);
        parameters.setOrderStatusWeight(order);

        log.info("Change to low contention workload:");
        for (TpccConsumer consumer : consumers) {

            consumer.getTerminal().change(getWarehouseForThread(consumer.getThreadIndex()), parameters.getPaymentWeight(), parameters.getOrderStatusWeight());
            //log.info(consumer.getName() + " terminal is " + consumer.terminal);
            log.info("randomContention for consumer: " + consumer.getThreadIndex());
        }
    }

    public synchronized final void randomContention(int payment, int order) {
        if (!running.get()) {
            return;
        }
        parameters.setPaymentWeight(payment);
        parameters.setOrderStatusWeight(order);

        log.info("Change to random contention workload:");
        for (TpccConsumer consumer : consumers) {
            consumer.getTerminal().change(-1, parameters.getPaymentWeight(), parameters.getOrderStatusWeight());
            //log.info(consumer.getName() + " terminal is " + consumer.terminal);
            log.info("randomContention for consumer: " + consumer.getThreadIndex());
        }

    }



    private String testIdString(long payment, long orderStatus, long threads) {
        return threads + "T_" + payment + "PA_" + orderStatus + "OS";
    }
}