package org.radargun.stages.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.producer.SyntheticProducer;
import org.radargun.stages.stressors.systems.System;
import org.radargun.stages.synthetic.SyntheticConsumer;
import org.radargun.stages.synthetic.SyntheticParameters;
import org.radargun.stages.synthetic.SyntheticXactFactory;
import org.radargun.stages.synthetic.runTimeDap.SyntheticXactFactory_RunTimeDaP;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/3/13 Time: 2:20 PM
 */
public class SyntheticStressor extends AbstractBenchmarkStressor<SyntheticParameters, SyntheticConsumer, SyntheticProducer, SyntheticXactFactory> {

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

   //For now, hardcoded, but we may want to use different Synthetic factories
   @Override
   protected SyntheticXactFactory createTransactionFactory(int threadIndex) {
      return new SyntheticXactFactory_RunTimeDaP(parameters, threadIndex);
   }

   @Override
   protected SyntheticConsumer createConsumer(int threadIndex) {
      log.trace("creating consumer (threadIndex: " + threadIndex + " )");
      SyntheticXactFactory factory = createTransactionFactory(threadIndex);
      return new SyntheticConsumer(cacheWrapper, threadIndex, system, benchmarkStage, this, parameters, factory);
   }
}
