package org.radargun.stages.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.producer.SyntheticProducer;
import org.radargun.stages.stressors.syntethic.SyntheticParameter;
import org.radargun.stages.stressors.syntethic.consumer.SyntheticConsumer;
import org.radargun.stages.stressors.systems.SystemType;
import org.radargun.stages.synthetic.SyntheticXactFactory;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/3/13
 * Time: 2:20 PM
 */
public class ScalableSyntheticStageStressor extends AbstractBenchmarkStressor<SyntheticParameter, SyntheticConsumer, SyntheticProducer, SyntheticXactFactory> {

    private static Log log = LogFactory.getLog(ScalableSyntheticStageStressor.class);

    public ScalableSyntheticStageStressor(CacheWrapper cacheWrapper, AbstractBenchmarkStage benchmarkStage, SystemType system, SyntheticParameter parameters) {
        super(cacheWrapper, benchmarkStage, system, parameters);
    }

    @Override
    protected void initialization() {
        // nothing to do
    }

    @Override
    protected void validateTransactionsWeight() {
        // nothing to do
    }

//    @Override
//    public int nextTransaction(int threadIndex) {
//        SyntheticXactFactory factory = this.consumers.get(threadIndex).getFactory();
//        return factory.chooseTransactionType(cacheWrapper.isTheMaster());
//    }

//    @Override
//    public ITransaction generateTransaction(RequestType type, int threadIndex) {
//        SyntheticXactFactory factory = this.consumers.get(threadIndex).getFactory();
//        return factory.createTransaction(type.getTransactionType());
//    }
//
//    @Override
//    public ITransaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster, int threadId) {
//        SyntheticConsumer consumer = this.consumers.get(threadId);
//        ITransaction transaction = consumer.getFactory().choiceTransaction(cacheWrapper.isPassiveReplication(), cacheWrapper.isTheMaster());
//        log.info("Closed system: starting a brand new transaction of type " + transaction.getType());
//        return transaction;
//    }

    @Override
    protected double getWriteWeight() {
        return parameters.getWritePercentage();
    }

    @Override
    protected double getReadWeight() {
        return parameters.getWritePercentage();
    }

    @Override
    protected SyntheticXactFactory createTransactionFactory(int threadIndex) {
        SyntheticXactFactory factory = new SyntheticXactFactory(parameters, threadIndex);
        return factory;
    }

    @Override
    protected SyntheticConsumer createConsumer(int threadIndex) {
        SyntheticXactFactory factory = createTransactionFactory(threadIndex);
        return new SyntheticConsumer(cacheWrapper, threadIndex, system, benchmarkStage, this, parameters, factory);
    }
}
