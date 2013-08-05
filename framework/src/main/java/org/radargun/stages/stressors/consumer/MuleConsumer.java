//package org.radargun.stages.stressors.consumer;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.radargun.CacheWrapper;
//import org.radargun.Transaction;
//import org.radargun.stages.AbstractBenchmarkStage;
//import org.radargun.stages.stressors.AbstractBenchmarkStressor;
//import org.radargun.stages.stressors.Parameters;
//import org.radargun.stages.stressors.producer.RequestType;
//import org.radargun.stages.stressors.systems.workloadGenerators.AbstractWorkloadGenerator;
//import org.radargun.stages.stressors.systems.MuleSystem;
//import org.radargun.stages.stressors.systems.OpenSystem;
//import org.radargun.stages.stressors.systems.System;
//
///**
// * Created by: Fabio Perfetti
// * E-mail: perfabio87@gmail.com
// * Date: 4/24/13
// */
//public class MuleConsumer extends Consumer<MuleSystem> {
//
//    private static Log log = LogFactory.getLog(MuleConsumer.class);
//
//    public MuleConsumer(CacheWrapper cacheWrapper,
//                        int threadIndex,
//                        MuleSystem system,
//                        AbstractBenchmarkStage stage,
//                        AbstractBenchmarkStressor stressor,
//                        Parameters parameters) {
//        super(cacheWrapper, threadIndex, system, stage, stressor, parameters);
//    }
//
//    @Override
//
//}
//
//
////                      COMMENTATO POICHé NON DOVREI MAI ENTRARE QUI
////                        if PassiveReplication so skip whether:
////                        a) master node && readOnly transaction
////                        b) slave node && write transaction
////                        boolean masterAndReadOnlyTx = cacheWrapper.isTheMaster() && tx.isReadOnly();
////                        boolean slaveAndWriteTx = (!cacheWrapper.isTheMaster() && !tx.isReadOnly());
////
////                        if (cacheWrapper.isPassiveReplication() && (masterAndReadOnlyTx || slaveAndWriteTx)) {
////                            continue;
////                        }