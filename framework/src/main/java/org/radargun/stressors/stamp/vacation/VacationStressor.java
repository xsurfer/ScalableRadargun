package org.radargun.stressors.stamp.vacation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.portings.stamp.vacation.Definitions;
import org.radargun.portings.stamp.vacation.Random;
import org.radargun.portings.stamp.vacation.transaction.DeleteCustomerOperation;
import org.radargun.portings.stamp.vacation.transaction.MakeReservationOperation;
import org.radargun.portings.stamp.vacation.transaction.UpdateTablesOperation;
import org.radargun.stressors.AbstractBenchmarkStressor;
import org.radargun.stressors.commons.StressorStats;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;

public class VacationStressor extends AbstractBenchmarkStressor<AbstractBenchmarkStressor.Consumer, StressorStats> {

    private static Log log = LogFactory.getLog(VacationStressor.class);

    private Random randomPtr;

    private int queryPerTx;

    /* percentUser is the percentage of MakeReservationOperation */
    private int percentUser;

    /* queryRange defines which part of the data can possibly be touched by the transactions */
    private int queryRange;

    /* readOnlyPerc is what percentage of MakeReservationOperation are read-only */
    private int readOnlyPerc;

    private int relations;


    /* ****************** */
    /* *** CONSTRUCTOR *** */
    /* ****************** */

    public VacationStressor(AbstractWorkloadGenerator loadGenerator) {
        super(loadGenerator);
        randomPtr = new Random();
        randomPtr.random_alloc();
    }


    /* ****************** */
    /* *** OVERRIDING *** */
    /* ****************** */

    @Override
    protected void initialization() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected RequestType nextTransaction() {

        int r = randomPtr.posrandom_generate() % 100;
        int action = selectAction(r, percentUser);
        RequestType requestType = new RequestType(System.nanoTime(),action);

        return requestType;
    }

    @Override
    protected Transaction generateTransaction(RequestType type, int threadIndex) {

        int action = type.transactionType;
        Transaction result = null;

        if (action == Definitions.ACTION_MAKE_RESERVATION) {
            result = new MakeReservationOperation(randomPtr, queryPerTx, queryRange, relations, readOnlyPerc);
        } else if (action == Definitions.ACTION_DELETE_CUSTOMER) {
            result = new DeleteCustomerOperation(randomPtr, queryRange, relations);
        } else if (action == Definitions.ACTION_UPDATE_TABLES) {
            result = new UpdateTablesOperation(randomPtr, queryPerTx, queryRange, relations);
        } else {
            assert (false);
        }

        return result;
    }

    @Override
    protected Transaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId) {
        int r = randomPtr.posrandom_generate() % 100;
        int action = selectAction(r, percentUser);
        RequestType requestType = new RequestType(System.nanoTime(),action);

        return generateTransaction(requestType, threadId);
    }

    @Override
    protected Map<String, String> processResults(List<AbstractBenchmarkStressor.Consumer> stressors) {

        for (Consumer tpccConsumer : consumers) {

        }

        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected double getWriteWeight() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected double getReadWeight() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void validateTransactionsWeight() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected Consumer createConsumer(int threadIndex) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void extractExtraStats(StressorStats totalStats, StressorStats singleStats) {

    }

    @Override
    protected void fillMapWithExtraStats(StressorStats totalStats, Map<String, String> results) {

    }

    @Override
    public StressorStats createStatsContainer() {
        return new StressorStats();  //To change body of implemented methods use File | Settings | File Templates.
    }



    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    public int selectAction(int r, int percentUser) {
        if (r < percentUser) {
            return Definitions.ACTION_MAKE_RESERVATION;
        } else if ((r & 1) == 1) {
            return Definitions.ACTION_DELETE_CUSTOMER;
        } else {
            return Definitions.ACTION_UPDATE_TABLES;
        }
    }

//    @Override
//    public Map<String, String> stress(CacheWrapper wrapper) {
//        //while (m_phase == TEST_PHASE) {
//            processTransaction(wrapper, generateTransaction());
//            this.throughput++;
//        }
//
//        Map<String, String> results = new LinkedHashMap<String, String>();
//
//        return results;
//    }

    /*
    private void processTransaction(CacheWrapper wrapper, VacationTransaction transaction) {
        boolean successful = true;

        while (true) {
            if (m_phase != TEST_PHASE) {
                this.throughput--;
                break;
            }
            cacheWrapper.startTransaction(transaction.isReadOnly());
            try {
                transaction.executeTransaction(cacheWrapper);
            } catch (Throwable e) {
                successful = false;
            }

            try {
                cacheWrapper.endTransaction(successful);

                if (!successful) {
                    setRestarts(getRestarts() + 1);
                }
            } catch (Throwable rb) {
                setRestarts(getRestarts() + 1);
                successful = false;
            }

            if (!successful) {
                successful = true;
            } else {
                break;
            }
        }
    }
    */




    /* ********************* */
    /* *** GETTER/SETTER *** */
    /* ********************* */

    public void setRelations(int relations) { this.relations = relations; }

    public void setPercentUser(int percentUser) { this.percentUser = percentUser; }

    public void setQueryPerTx(int queryPerTx) { this.queryPerTx = queryPerTx; }

    public void setQueryRange(int queryRange) { this.queryRange = queryRange; }

    public void setReadOnlyPerc(int readOnlyPerc) { this.readOnlyPerc = readOnlyPerc; }

    public void setCacheWrapper(CacheWrapper cacheWrapper) { this.cacheWrapper = cacheWrapper; }

//    public long getThroughput() { return this.throughput; }

//    public long getRestarts() { return restarts; }
//    public void setRestarts(long restarts) { this.restarts = restarts; }

    //public void setPhase(int shutdownPhase) { this.m_phase = shutdownPhase; }

}
