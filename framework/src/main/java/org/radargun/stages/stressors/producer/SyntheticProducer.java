package org.radargun.stages.stressors.producer;

import org.radargun.TransactionFactory;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.syntethic.SyntheticParameters;

import java.util.Random;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/3/13 Time: 5:33 PM
 */
public class SyntheticProducer extends Producer {

   private Random rnd = new Random();
   private SyntheticParameters params;
   private Producer producer;

   public SyntheticProducer(int _id,
                            AbstractBenchmarkStressor stressor,
                            Producer producer,
                            SyntheticParameters params,
                            TransactionFactory factory) {
      super(_id, stressor, params, factory);
      this.producer = producer;
      this.params = params;

   }

   @Override
   protected double getSleepTime() {
      return producer.getSleepTime();
   }

   @Override
   protected void sleep() {
      producer.sleep();
   }

   @Override
   protected RequestType createRequestType(int reqType) {
      return producer.createRequestType(reqType);
   }

   @Override
   public void doNotify() {
      producer.doNotify();
   }

}