package org.radargun.stressors.stamp.vacation;

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
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.commons.StressorStats;
import org.radargun.stressors.consumer.Consumer;
import org.radargun.stressors.producer.RequestType;
import org.radargun.stressors.tpcc.TpccStressorParameter;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;
import org.radargun.workloadGenerator.SystemType;

public class VacationStressor extends BenchmarkStressor<VacationStressorParameter, Consumer> {

    private static Log log = LogFactory.getLog(VacationStressor.class);

    private Random randomPtr;




    /* ****************** */
    /* *** CONSTRUCTOR *** */
    /* ****************** */

    public VacationStressor(CacheWrapper cacheWrapper, AbstractBenchmarkStage benchmarkStage, SystemType system, VacationStressorParameter parameters) {
        super(cacheWrapper, benchmarkStage, system, parameters);
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
    public RequestType nextTransaction() {

        int r = randomPtr.posrandom_generate() % 100;
        int action = selectAction(r, parameters.getPercentUser());
        RequestType requestType = new RequestType(System.nanoTime(),action);

        return requestType;
    }

    @Override
    public Transaction generateTransaction(RequestType type, int threadIndex) {

        int action = type.transactionType;
        Transaction result = null;

        if (action == Definitions.ACTION_MAKE_RESERVATION) {
            result = new MakeReservationOperation(randomPtr, parameters.getQueryPerTx(), parameters.getQueryRange(), parameters.getRelations(), parameters.getReadOnlyPerc());
        } else if (action == Definitions.ACTION_DELETE_CUSTOMER) {
            result = new DeleteCustomerOperation(randomPtr, parameters.getQueryRange(), parameters.getRelations());
        } else if (action == Definitions.ACTION_UPDATE_TABLES) {
            result = new UpdateTablesOperation(randomPtr, parameters.getQueryPerTx(), parameters.getQueryRange(), parameters.getRelations());
        } else {
            assert (false);
        }

        return result;
    }

    @Override
    public Transaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId) {
        int r = randomPtr.posrandom_generate() % 100;
        int action = selectAction(r, parameters.getPercentUser());
        RequestType requestType = new RequestType(System.nanoTime(),action);

        return generateTransaction(requestType, threadId);
    }

    @Override
    protected double getWriteWeight() {
        double writeWeight = 1 - getReadWeight();
        return writeWeight;
    }

    @Override
    protected double getReadWeight() {
        double readWeight = (parameters.getPercentUser()*parameters.getReadOnlyPerc()) / 100D;
        return readWeight;
    }

    @Override
    protected void validateTransactionsWeight() {
        int sum = parameters.getPercentUser();
        if (sum < 0 || sum > 100) {
            throw new IllegalArgumentException("The sum of the transactions weights must be higher or equals than zero " +
                    "and less or equals than one hundred");
        }
    }

    @Override
    protected Consumer createConsumer(int threadIndex) {
        return new Consumer(cacheWrapper, threadIndex, system, benchmarkStage, this, parameters);
    }

    /*
    @Override
    protected void extractExtraStats(StressorStats totalStats, StressorStats singleStats) { }

    @Override
    protected void fillMapWithExtraStats(StressorStats totalStats, Map<String, String> results) { }

    @Override
    public StressorStats createStatsContainer() {
        return new StressorStats();  //To change body of implemented methods use File | Settings | File Templates.
    }
    */


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

//    public void setRelations(int relations) { this.relations = relations; }
//
//    public void setPercentUser(int percentUser) { this.percentUser = percentUser; }
//
//    public void setQueryPerTx(int queryPerTx) { this.queryPerTx = queryPerTx; }
//
//    public void setQueryRange(int queryRange) { this.queryRange = queryRange; }
//
//    public void setReadOnlyPerc(int readOnlyPerc) { this.readOnlyPerc = readOnlyPerc; }

//  public void setCacheWrapper(CacheWrapper cacheWrapper) { this.cacheWrapper = cacheWrapper; }

//    public long getThroughput() { return this.throughput; }

//    public long getRestarts() { return restarts; }
//    public void setRestarts(long restarts) { this.restarts = restarts; }

    //public void setPhase(int shutdownPhase) { this.m_phase = shutdownPhase; }

}
