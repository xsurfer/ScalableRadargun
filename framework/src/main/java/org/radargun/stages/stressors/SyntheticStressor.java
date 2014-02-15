package org.radargun.stages.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.producer.SyntheticProducer;
import org.radargun.stages.stressors.syntethic.SyntheticParameters;
import org.radargun.stages.stressors.syntethic.consumer.SyntheticConsumer;
import org.radargun.stages.stressors.systems.System;
import org.radargun.stages.synthetic.SyntheticDistinctXactFactory;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/3/13 Time: 2:20 PM
 */
public class SyntheticStressor extends AbstractBenchmarkStressor<SyntheticParameters, SyntheticConsumer, SyntheticProducer, SyntheticDistinctXactFactory> {

   private static Log log = LogFactory.getLog(SyntheticStressor.class);

   public SyntheticStressor(CacheWrapper cacheWrapper, AbstractBenchmarkStage benchmarkStage, System system, SyntheticParameters parameters) {
      super(cacheWrapper, benchmarkStage, system, parameters);
   }

   @Override
   protected void initialization() {
      log.trace("initialization: nothing to do");
      // nothing to do
   }

   @Override
   protected void validateTransactionsWeight() {
      log.trace("validateTransactionsWeight: nothing to do");
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

   public void changeUpdateTx(int writePercentage, int updateXactReads, int updateXactWrites) {

      if (!running.get()) {
         return;
      }
      parameters.setWritePercentage(writePercentage);
      parameters.setUpdateXactReads(updateXactReads);
      parameters.setUpdateXactWrites(updateXactWrites);

      log.info("Changing update tx:");
      for (SyntheticConsumer consumer : consumers) {
         //consumer.getTerminal().change(1, parameters.getPaymentWeight(), parameters.getOrderStatusWeight());
         //log.info(consumer.getName() + " terminal is " + consumer.getFactory());
         log.info("UpdateTx changed for consumer: " + consumer.getThreadIndex());
      }

   }

   public void changeReadOnlyTx(int readOnlyPercentage, int readOnlyXactSize) {
      if (!running.get()) {
         return;
      }
      parameters.setWritePercentage(100 - readOnlyPercentage);
      parameters.setReadOnlyXactSize(readOnlyXactSize);

      log.info("Changing readOnly tx:");
      for (SyntheticConsumer consumer : consumers) {
         //consumer.getTerminal().change(1, parameters.getPaymentWeight(), parameters.getOrderStatusWeight());
         //log.info(consumer.getName() + " terminal is " + consumer.getFactory());
         log.info("UpdateTx changed for consumer: " + consumer.getThreadIndex());
      }
   }


   @Override
   public double getWriteWeight() {
      return parameters.getWritePercentage();
   }

   @Override
   public double getReadWeight() {
      return 100 - parameters.getWritePercentage();
   }

   @Override
   protected SyntheticDistinctXactFactory createTransactionFactory(int threadIndex) {
      SyntheticDistinctXactFactory factory = new SyntheticDistinctXactFactory(parameters, threadIndex);
      return factory;
   }

   @Override
   protected SyntheticConsumer createConsumer(int threadIndex) {
      log.trace("creating consumer (threadIndex: " + threadIndex + " )");
      SyntheticDistinctXactFactory factory = createTransactionFactory(threadIndex);
      return new SyntheticConsumer(cacheWrapper, threadIndex, system, benchmarkStage, this, parameters, factory);
   }
}
