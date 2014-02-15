package org.radargun.stages.stressors.producer;

import org.radargun.TransactionFactory;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.Parameters;

/**
 * Created by: Fabio Perfetti E-mail: perfabio87@gmail.com Date: 4/18/13
 */

public class OpenProducer extends Producer {
   protected final ProducerRate rate;

   public OpenProducer(AbstractBenchmarkStressor stressor, ProducerRate rate, int id, Parameters parameters, TransactionFactory factory) {
      super(id, stressor, parameters, factory);
      this.rate = rate;
   }

   @Override
   protected double getSleepTime() {
      return rate.getLambda();
   }

   @Override
   protected void sleep() {
      rate.sleep();
   }

   @Override
   protected RequestType createRequestType(int reqType) {
      return new RequestType(System.nanoTime(), reqType);
   }

   @Override
   public void doNotify() {
      // nop
   }

}