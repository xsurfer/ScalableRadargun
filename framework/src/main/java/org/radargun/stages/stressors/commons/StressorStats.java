package org.radargun.stages.stressors.commons;

import org.radargun.TransactionDecorator;
import org.radargun.stages.stressors.exceptions.ApplicationException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/2/13
 */
public class StressorStats {

    private Map<String, Long> statistics = new HashMap<String, Long>();

    public void put(String key, Long val){
        statistics.put(key, val);
    }

    public long get(String key){
        Long val = statistics.get(key);
        if( val == null ){
            val = new Long(0);
            statistics.put(key, val);
        }
        return val;
    }

    public Long inc(String key){
        Long val = get(key);
        val++;
        statistics.put(key, val);
        return val;
    }

    public Long inc(String key, Long q){
        Long val = get(key);
        val += q;
        statistics.put(key, val);
        return val;
    }

    /* *********************** */
    /* *** SINGLE TX STATS *** */
    /* *********************** */
    public static final String START_EXEC_TIMESTAMP = "startTimeStamp";
    public static final String END_EXEC_TIMESTAMP = "endTimeStamp";

    public static final String START_COMMIT_TIMESTAMP = "startCommitTimestamp";
    public static final String END_COMMIT_TIMESTAMP = "endCommitTimestamp";


    /* ********************* */
    /* *** GENERIC STATS *** */
    /* ********************* */

    public static final String READS = "reads";
    public static final String WRITES = "writes";
    public static final String NR_FAILURES = "nrFailures";
    public static final String NR_WR_FAILURES = "nrWrFailures";
    public static final String NR_RD_FAILURES = "nrRdFailures";
    public static final String NR_WR_FAILURES_ON_COMMIT = "nrWrFailuresOnCommit";
    public static final String APP_FAILURES = "appFailures";
    public static final String DURATION = "duration";
    public static final String READ_DURATION = "readDuration";
    public static final String WRITE_DURATION = "writeDuration";
    public static final String SUCCESSFUL_COMMIT_WRITE_DURATION = "successfulCommitWriteDuration";
    public static final String ABORTED_COMMIT_WRITE_DURATION = "abortedCommitWriteDuration";
    public static final String COMMIT_WRITE_DURATION = "commitWriteDuration";
    public static final String WRITE_SERVICE_TIME = "writeServiceTime";
    public static final String READ_SERVICE_TIME = "readServiceTime";
    public static final String SUCCESSFUL_WRITE_DURATION = "successfulWriteDuration";
    public static final String SUCCESSFUL_READ_DURATION = "successfulReadDuration";

    /* ******************* */
    /* *** QUEUE STATS *** */
    /* ******************* */

    public static final String NUM_WRITE_DEQUEUED = "numWriteDequeued";
    public static final String NUM_READ_DEQUEUED = "numReadDequeued";
    public static final String WRITE_IN_QUEUE_TIME = "writeInQueueTime";
    public static final String READ_IN_QUEUE_TIME = "readInQueueTime";
    public static final String LOCAL_TIMEOUT = "localTimeout";
    public static final String REMOTE_TIMEOUT = "remoteTimeout";
    public static final String NUM_BACK_OFFS = "numBackOffs";
    public static final String BACKED_OFF_TIME = "backedOffTime";


    /* ****************************** */
    /* *** PERSONAL STATS METHODS *** */
    /* ****************************** */


    /* *** START/END HANDLERS *** */
    public void handleStartsTx(TransactionDecorator tx){}
    public final void _handleStartsTx(TransactionDecorator tx){
        tx.setStartTimestamp(System.nanoTime());

        handleStartsTx(tx);
    }

    public void handleEndTx(TransactionDecorator tx, boolean successful){}
    public final void _handleEndTx(TransactionDecorator tx, boolean successful){

        if (!tx.isReadOnly()) { //write

            put( StressorStats.WRITE_DURATION, tx.getEndTimestamp() - tx.getDequeueTimestamp() );
            put(StressorStats.WRITE_SERVICE_TIME, tx.getEndTimestamp() - tx.getStartTimestamp());
            if (successful) {
                put( StressorStats.SUCCESSFUL_WRITE_DURATION, tx.getEndTimestamp() - tx.getStartTimestamp() );
                inc(WRITES);
            }

            if(successful){
                put(SUCCESSFUL_COMMIT_WRITE_DURATION, tx.getEndTimestamp()-get(START_COMMIT_TIMESTAMP) );
            } else {
                put(ABORTED_COMMIT_WRITE_DURATION, tx.getEndTimestamp() - get(START_COMMIT_TIMESTAMP) );
            }
            put(COMMIT_WRITE_DURATION,tx.getEndTimestamp() - get(START_COMMIT_TIMESTAMP));

        } else {
            put(StressorStats.READ_DURATION, tx.getEndTimestamp() - tx.getDequeueTimestamp());
            put(StressorStats.READ_SERVICE_TIME, tx.getEndTimestamp() - tx.getStartTimestamp());
            if (successful) {
                put( StressorStats.SUCCESSFUL_READ_DURATION, tx.getEndTimestamp() - tx.getStartTimestamp() );
                inc(READS);
            }
        }

        handleEndTx(tx, successful);
    }



    /* *** LOCAL HANDLERS *** */
    public void handleSuccessLocalTx(TransactionDecorator tx){}
    public final void _handleSuccessLocalTx(TransactionDecorator tx){
        handleSuccessLocalTx(tx);

        put(START_COMMIT_TIMESTAMP, System.nanoTime());
    }

    public void handleAbortLocalTx(TransactionDecorator tx, Throwable e){}
    public final void _handleAbortLocalTx(TransactionDecorator tx, Throwable e, boolean isCacheTimeout){

        inc(StressorStats.NR_FAILURES);

        if(isCacheTimeout)
            inc(StressorStats.LOCAL_TIMEOUT);


        if (!tx.isReadOnly()) {
            inc(StressorStats.NR_WR_FAILURES);
        } else {
            inc(StressorStats.NR_RD_FAILURES);
        }

        if (e instanceof ApplicationException) {
            inc(APP_FAILURES);
        }

        handleAbortLocalTx(tx, e);
    }



    /* *** REMOTE HANDLERS *** */
    public void handleSuccessRemoteTx(TransactionDecorator tx){}
    public final void _handleSuccessRemoteSuccessTx(TransactionDecorator tx){
        tx.setEndTimestamp(System.nanoTime());

        handleSuccessRemoteTx(tx);
    }

    public void handleAbortRemoteTx(TransactionDecorator tx, Throwable e){}
    public final void _handleAbortRemoteTx(TransactionDecorator tx, Throwable e){
        inc(StressorStats.NR_FAILURES);
        inc(StressorStats.REMOTE_TIMEOUT);

        if (!tx.isReadOnly()) {
            inc(StressorStats.NR_WR_FAILURES);
            inc(StressorStats.NR_WR_FAILURES_ON_COMMIT);
        } else {
            inc(StressorStats.NR_RD_FAILURES);
        }

        handleAbortRemoteTx(tx, e);
    }



    /* *** OTHER HANDLERS *** */
    public void handleQueueTx(TransactionDecorator tx){}
    public final void _handleQueueTx(TransactionDecorator tx){
        long queuingTime = tx.getDequeueTimestamp() - tx.getEnqueueTimestamp();
        if (tx.isReadOnly()) {
            inc(StressorStats.NUM_READ_DEQUEUED);
            put(StressorStats.READ_IN_QUEUE_TIME, queuingTime );
        } else {
            inc(StressorStats.NUM_WRITE_DEQUEUED);
            put(StressorStats.WRITE_IN_QUEUE_TIME, queuingTime);
        }

        handleQueueTx(tx);
    }




    /* ************************** */
    /* *** EVALUATION METHODS *** */
    /* ************************** */

    public double evalRequestPerSec(){
        long duration = get(StressorStats.DURATION);
        if( duration == 0 )
            return 0;
        double requestPerSec = (get(StressorStats.READS) + get(StressorStats.WRITES)) / (duration / 1000.0);
        return requestPerSec;
    }

    public double evalWrtPerSec(){
        long duration = get(StressorStats.DURATION);
        if( duration == 0 )
            return 0;
        return (double) get(StressorStats.WRITES) / (duration / 1000.0);
    }

    public double evalRdPerSec(){
        long duration = get(StressorStats.DURATION);
        if( duration == 0 )
            return 0;
        return (double) get(StressorStats.READS) / (duration / 1000.0);
    }

    public double evalAvgSuccessfulDuration(){
        long sum = get(READS) + get(WRITES);
        if (sum == 0)
            return 0;
        return (double) (get(SUCCESSFUL_WRITE_DURATION) + get(SUCCESSFUL_READ_DURATION)) / (sum);
    }

    public double evalAvgSuccessfulReadDuration(){
        long reads = get(READS);
        if (reads == 0)
            return 0;
        return (double) ( get(SUCCESSFUL_READ_DURATION) ) / (reads);
    }

    public double evalAvgSuccessfulWriteDuration(){
        long writes = get(WRITES);
        if (writes == 0)
            return 0;
        return (double) ( get(SUCCESSFUL_WRITE_DURATION) ) / (writes);
    }

    public double evalAvgSuccessfulCommitWriteDuration(){
        long writes = get(WRITES);
        if (writes == 0)
            return 0;
        return (double) ( get(SUCCESSFUL_COMMIT_WRITE_DURATION) ) / (writes);
    }

    public double evalAvgAbortedCommitWriteDuration(){
        long denom = get(NR_WR_FAILURES_ON_COMMIT);
        if (denom == 0)
            return 0;
        return (double) ( get(ABORTED_COMMIT_WRITE_DURATION) ) / (denom);
    }

    public double evalAvgCommitWriteDuration(){
        long denom = get(NR_WR_FAILURES_ON_COMMIT) + get(WRITES);
        if (denom == 0)
            return 0;
        return (double) ( get(COMMIT_WRITE_DURATION) ) / (denom);
    }

    public double evalAvgRdServiceTime(){
        long denom = get(READS) + get(NR_RD_FAILURES);
        if (denom == 0)
            return 0;
        return (double) ( get(READ_SERVICE_TIME) ) / (denom);
    }

    public double evalAvgWrServiceTime(){
        long denom = get(WRITES) + get(NR_WR_FAILURES);
        if (denom == 0)
            return 0;
        return (double) ( get(WRITE_SERVICE_TIME) ) / (denom);
    }

    public double evalAvgWrInQueueTime(){
        long denom = get(NUM_WRITE_DEQUEUED);
        if (denom == 0)
            return 0;
        return (double) ( get(WRITE_IN_QUEUE_TIME) ) / (denom);
    }

    public double evalAvgRdInQueueTime(){
        long denom = get(NUM_READ_DEQUEUED);
        if (denom == 0)
            return 0;
        return (double) ( get(READ_IN_QUEUE_TIME) ) / (denom);
    }

    public double evalAvgBackoff(){
        long denom = get(NUM_BACK_OFFS);
        if (denom == 0)
            return 0;
        return (double) ( get(BACKED_OFF_TIME) ) / (denom);
    }

}
