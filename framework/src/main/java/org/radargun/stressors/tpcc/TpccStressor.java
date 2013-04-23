package org.radargun.stressors.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.Transaction;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.portings.tpcc.TpccTerminal;
import org.radargun.portings.tpcc.TpccTools;
import org.radargun.stressors.producer.RequestType;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;


import java.util.*;


/**
 * On multiple threads executes implementations of TPC-C Transaction Profiles against the CacheWrapper, and returns the
 * result as a Map.
 *
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 * @author Fabio Perfetti (refactored)
 */

public class TpccStressor extends BenchmarkStressor<TpccStressor.TpccConsumer, TpccStats> {


    private static Log log = LogFactory.getLog(TpccStressor.class);

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

    private final List<Integer> listLocalWarehouses = new LinkedList<Integer>();


    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public TpccStressor(AbstractWorkloadGenerator loadGenerator) {
        super(loadGenerator);
    }

    /* ****************** */
    /* *** OVERRIDING *** */
    /* ****************** */

    @Override
    public TpccStats createStatsContainer(){
        return new TpccStats();
    }

    @Override
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

    @Override
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
        results.put("TEST_ID", this.testIdString(paymentWeight, orderStatusWeight, numOfThreads));

    }


    @Override
    protected void initialization(){
        updateNumberOfItemsInterval();
        initializeToolsParameters();
        calculateLocalWarehouses();
    }

    @Override
    public RequestType nextTransaction() {
        TpccTerminal terminal = new TpccTerminal(paymentWeight, orderStatusWeight, nodeIndex, 0);
        return new RequestType( System.nanoTime(), terminal.chooseTransactionType(
                                                                                  cacheWrapper.isPassiveReplication(),
                                                                                  cacheWrapper.isTheMaster()
                                                                                ) );
    }

    @Override
    protected Transaction generateTransaction(RequestType type, int threadIndex) {

        TpccConsumer consumer = this.consumers.get(threadIndex);
        Transaction transaction = consumer.getTerminal().createTransaction(type.transactionType, threadIndex);
        return transaction;
    }

    @Override
    public Transaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId) {
        TpccConsumer consumer = this.consumers.get(threadId);
        Transaction transaction = consumer.getTerminal().choiceTransaction(cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster(), threadId);
        log.info("Closed system: starting a brand new transaction of type " + transaction.getType());
        return transaction;
    }

    @Override
    protected TpccConsumer createConsumer(int threadIndex) {
        int localWarehouse = getWarehouseForThread(threadIndex);
        return new TpccConsumer(localWarehouse, threadIndex, nodeIndex, paymentWeight, orderStatusWeight);
    }



    @Override
    protected void validateTransactionsWeight() {
        int sum = orderStatusWeight + paymentWeight;
        if (sum < 0 || sum > 100) {
            throw new IllegalArgumentException("The sum of the transactions weights must be higher or equals than zero " +
                    "and less or equals than one hundred");
        }
    }

    @Override
    protected double getWriteWeight() {
        double writeWeight = Math.max(100 - orderStatusWeight, paymentWeight) / 100D;
        return writeWeight;
    }

    @Override
    protected double getReadWeight() {
        double readWeight = orderStatusWeight / 100D;
        return readWeight;
    }




    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    private void updateNumberOfItemsInterval() {
        if (numberOfItemsInterval == null) {
            return;
        }
        String[] split = numberOfItemsInterval.split(",");

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
            log.error("Error", e);
        }
    }






    /* ********************* */
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



    @Override
    public String toString() {
        return "TpccStressor{" +
                "updateTimes=" + perThreadSimulTime +
                ", paymentWeight=" + paymentWeight +
                ", orderStatusWeight=" + orderStatusWeight +
                ", accessSameWarehouse=" + accessSameWarehouse +
                ", numSlaves=" + numSlaves +
                ", nodeIndex=" + nodeIndex +
                ", numOfThreads=" + numOfThreads +
                ", numberOfItemsInterval=" + numberOfItemsInterval +
                ", statsSamplingInterval=" + statsSamplingInterval +
                '}';
    }


    private void calculateLocalWarehouses() {
        if (accessSameWarehouse) {
            TpccTools.selectLocalWarehouse(numSlaves, nodeIndex, listLocalWarehouses);
            if (log.isDebugEnabled()) {
                log.debug("Find the local warehouses. Number of Warehouses=" + TpccTools.NB_WAREHOUSES + ", number of slaves=" +
                        numSlaves + ", node index=" + nodeIndex + ".Local warehouses are " + listLocalWarehouses);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Local warehouses are disabled. Choose a random warehouse in each transaction");
            }
        }
    }

   /*
    * For the review, the workload is the following:
    * 
    * high contention: change(1, 85, 10);
    * low contention: change(nodeIndex + 1, 45, 50); 
    */

//    public synchronized final void highContention(int payment, int order) {
//        if (!running.get()) {
//            return;
//        }
//        paymentWeight = payment;
//        orderStatusWeight = order;
//
//        log.info("Change to high contention workload:");
//        for (Stressor stressor : consumers) {
//            stressor.terminal.change(1, paymentWeight, orderStatusWeight);
//            log.info(stressor.getName() + " terminal is " + stressor.terminal);
//        }
//        for (Producer producer : producers) {
//            producer.terminal.change(1, paymentWeight, orderStatusWeight);
//            log.info(producer.getName() + " terminal is " + producer.terminal);
//        }
//    }

//    public synchronized final void lowContention(int payment, int order) {
//        if (!running.get()) {
//            return;
//        }
//        if (listLocalWarehouses.isEmpty()) {
//            TpccTools.selectLocalWarehouse(numSlaves, nodeIndex, listLocalWarehouses);
//        }
//        paymentWeight = payment;
//        orderStatusWeight = order;
//
//        log.info("Change to low contention workload:");
//        for (Stressor stressor : consumers) {
//            stressor.terminal.change(getWarehouseForThread(stressor.threadIndex), paymentWeight, orderStatusWeight);
//            log.info(stressor.getName() + " terminal is " + stressor.terminal);
//        }
//        for (Producer producer : producers) {
//            //in the producers, the warehouse is not needed
//            producer.terminal.change(-1, paymentWeight, orderStatusWeight);
//            log.info(producer.getName() + " terminal is " + producer.terminal);
//        }
//    }

//    public synchronized final void randomContention(int payment, int order) {
//        if (!running.get()) {
//            return;
//        }
//        paymentWeight = payment;
//        orderStatusWeight = order;
//
//        log.info("Change to random contention workload:");
//        for (Stressor stressor : consumers) {
//            stressor.terminal.change(-1, paymentWeight, orderStatusWeight);
//            log.info(stressor.getName() + " terminal is " + stressor.terminal);
//        }
//        for (Producer producer : producers) {
//            producer.terminal.change(-1, paymentWeight, orderStatusWeight);
//            log.info(producer.getName() + " terminal is " + producer.terminal);
//        }
//    }

    private int getWarehouseForThread(int threadIdx) {
        return listLocalWarehouses.isEmpty() ? -1 : listLocalWarehouses.get(threadIdx % listLocalWarehouses.size());
    }

    public synchronized final double getExpectedWritePercentage() {
        return 1.0 - (orderStatusWeight / 100.0);
    }

    public synchronized final int getPaymentWeight() {
        return paymentWeight;
    }

    public synchronized final int getOrderStatusWeight() {
        return orderStatusWeight;
    }



    private String testIdString(long payment, long orderStatus, long threads) {
        return threads + "T_" + payment + "PA_" + orderStatus + "OS";
    }


    /* ************************** */
    /* *** TpccConsumer CLASS *** */
    /* ************************** */

    public class TpccConsumer extends BenchmarkStressor<?, TpccStats>.Consumer {

        private final TpccTerminal terminal;

        public TpccConsumer(int localWarehouseID, int threadIndex, int nodeIndex, double paymentWeight, double orderStatusWeight) {
            super(threadIndex);

            stats = new TpccStats();
            this.threadIndex = threadIndex;
            this.terminal = new TpccTerminal(paymentWeight, orderStatusWeight, nodeIndex, localWarehouseID);
        }

        public TpccTerminal getTerminal(){ return terminal; }

    }


   /* ************************** */
   /* ***** PRODUCER CLASS ***** */
   /* ************************** */

//    private class Producer extends Thread {
//        private final ProducerRate rate;
//        private final TpccTerminal terminal;
//        private boolean running = false;
//
//        public Producer(ProducerRate rate, int id) {
//            super("Producer-" + id);
//            setDaemon(true);
//            this.rate = rate;
//            this.terminal = new TpccTerminal(paymentWeight, orderStatusWeight, nodeIndex, 0);
//        }
//
//        public void run() {
//            if (log.isDebugEnabled()) {
//                log.debug("Starting " + getName() + " with rate of " + rate.getLambda());
//            }
//            while (assertRunning()) {
//                queue.offer(new RequestType(System.nanoTime(), terminal.chooseTransactionType(
//                        cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster()
//                )));
//                countJobs.incrementAndGet();
//                rate.sleep();
//            }
//        }
//
//        private synchronized boolean assertRunning() {
//            return running;
//        }
//
//        @Override
//        public synchronized void start() {
//            if (running) return;
//            running = true;
//            super.start();
//        }
//
//        @Override
//        public synchronized void interrupt() {
//            if (!running) return;
//            running = false;
//            super.interrupt();
//        }
//    }

}
