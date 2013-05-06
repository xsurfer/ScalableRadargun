package org.radargun.stressors.consumer;

import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.StressorParameter;
import org.radargun.stressors.producer.RequestType;
import org.radargun.workloadGenerator.ClosedSystem;
import org.radargun.workloadGenerator.MuleSystem;
import org.radargun.workloadGenerator.SystemType;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/24/13
 */
public class ClosedConsumer extends Consumer<ClosedSystem> {


    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */

    public ClosedConsumer(CacheWrapper cacheWrapper,
                          int threadIndex,
                          ClosedSystem system,
                          AbstractBenchmarkStage stage,
                          BenchmarkStressor stressor,
                          StressorParameter parameters) {

        super(cacheWrapper, threadIndex, system, stage, stressor, parameters);
    }

    @Override
    protected void consume() {

        Transaction tx;
        long dequeueTimestamp = -1;
        boolean successful = true;

        while (assertRunning()) {
            /* 1- Extracting && generating request from queue */
            tx = null;
            dequeueTimestamp = -1;

            RequestType request = stressor.takeFromQueue();
            dequeueTimestamp = System.nanoTime();

            tx = stage.generateTransaction(request, threadIndex);
            tx.setEnqueueTimestamp(request.enqueueTimestamp);
            tx.setDequeueTimestamp(dequeueTimestamp);

//                      COMMENTATO POICHé NON DOVREI MAI ENTRARE QUI
//                        if PassiveReplication so skip whether:
//                        a) master node && readOnly transaction
//                        b) slave node && write transaction
//                        boolean masterAndReadOnlyTx = cacheWrapper.isTheMaster() && tx.isReadOnly();
//                        boolean slaveAndWriteTx = (!cacheWrapper.isTheMaster() && !tx.isReadOnly());
//
//                        if (cacheWrapper.isPassiveReplication() && (masterAndReadOnlyTx || slaveAndWriteTx)) {
//                            continue;
//                        }


            /* updating queue stats */
            stats._handleQueueTx(tx);

            /* 2- Executing the transaction */
            successful = processTransaction(tx); /* it executes the retryOnAbort (if enabled) */
            stats._handleEndTx(tx, successful);

            // notify the producer
            tx.notifyProducer();

            blockIfInactive();
        }
    }

}
