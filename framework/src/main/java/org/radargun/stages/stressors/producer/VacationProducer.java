package org.radargun.stages.stressors.producer;

import org.radargun.TransactionFactory;
import org.radargun.portings.stamp.vacation.Random;
import org.radargun.stages.stressors.stamp.vacation.VacationParameter;
import org.radargun.stages.stressors.stamp.vacation.VacationStressor;


/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/3/13 Time: 5:33 PM
 */
public class VacationProducer extends Producer<VacationStressor, VacationParameter> {

   private Random rnd = new Random();
   private Producer producer;

   public VacationProducer(int _id,
                           VacationStressor stressor,
                           Producer producer,
                           VacationParameter params,
                           TransactionFactory factory) {
      super(_id, stressor, params, factory);
      this.producer = producer;

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