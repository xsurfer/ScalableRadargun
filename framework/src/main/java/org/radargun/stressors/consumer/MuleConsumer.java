package org.radargun.stressors.consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.Transaction;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.StressorParameter;
import org.radargun.stressors.producer.RequestType;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;
import org.radargun.workloadGenerator.MuleSystem;
import org.radargun.workloadGenerator.OpenSystem;
import org.radargun.workloadGenerator.SystemType;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/24/13
 */
public class MuleConsumer extends Consumer<MuleSystem> {

    private static Log log = LogFactory.getLog(MuleConsumer.class);

    public MuleConsumer(CacheWrapper cacheWrapper,
                        int threadIndex,
                        MuleSystem system,
                        AbstractBenchmarkStage stage,
                        BenchmarkStressor stressor,
                        StressorParameter parameters) {
        super(cacheWrapper, threadIndex, system, stage, stressor, parameters);
    }

    @Override
    protected void consume(){

        Transaction tx;
        long dequeueTimestamp = -1;
        boolean successful = true;

        while (assertRunning()) {
            /* 1- Extracting && generating request from queue */
            tx = null;
            dequeueTimestamp = -1;

            //log.info("Mule system: starting a brand new transaction of type " + tx.getType());
            tx = stage.choiceTransaction(cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster(), threadIndex);

            /* 2- Executing the transaction */
            successful = processTransaction(tx); /* it executes the retryOnAbort (if enabled) */
            tx.setDequeueTimestamp(tx.getStartTimestamp()); // No queuing time

            stats._handleEndTx(tx, successful);

            // sleep think time
            try {
                Thread.sleep(system.getThinkTime());
            } catch (InterruptedException e) {
                log.warn("Interrupt!");
            }

            blockIfInactive();
        }
    }
}


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