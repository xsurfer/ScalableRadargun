//package org.radargun.stressors.consumer;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.radargun.CacheWrapper;
//import org.radargun.Transaction;
//import org.radargun.stages.AbstractBenchmarkStage;
//import org.radargun.stressors.BenchmarkStressor;
//import org.radargun.stressors.StressorParameter;
//import org.radargun.stressors.producer.RequestType;
//import org.radargun.workloadGenerator.*;
//
///**
// * Created by: Fabio Perfetti
// * E-mail: perfabio87@gmail.com
// * Date: 4/19/13
// */
//public class OpenConsumer extends Consumer<OpenSystem> {
//
//    private static Log log = LogFactory.getLog(OpenConsumer.class);
//
//
//    public long commit_start = 0L;
//
//    boolean takeStats;
//
//
//    /* ******************* */
//    /* *** CONSTRUCTOR *** */
//    /* ******************* */
//
//    public OpenConsumer(CacheWrapper cacheWrapper,
//                        int threadIndex,
//                        OpenSystem system,
//                        AbstractBenchmarkStage stage,
//                        BenchmarkStressor stressor,
//                        StressorParameter parameters) {
//
//        super(cacheWrapper, threadIndex, system, stage, stressor, parameters);
//    }
//
//
//    /* *************** */
//    /* *** METHODS *** */
//    /* *************** */
//
//    @Override
//
//
//
//}
