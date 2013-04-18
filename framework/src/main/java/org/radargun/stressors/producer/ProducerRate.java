package org.radargun.stressors.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;

import java.lang.reflect.Constructor;

/**
 * This class handles the sleep time for the producer rates
 *
 * @author Pedro Ruivo
 * @author Diego Didona
 * @since 1.1
 */
public abstract class ProducerRate {

   private static Log log = LogFactory.getLog(ProducerRate.class);
   private final double lambda;

    /**
     * Inner class to for exceptions
     */
    public static class ProducerRateException extends Exception{
        public ProducerRateException(Exception e) { super(e); }
    }

    public static ProducerRate createInstance(AbstractWorkloadGenerator.RateDistribution producerRateEnum, double producerLambda) throws ProducerRateException{
        ProducerRate obj;
        String producerRateString = producerRateEnum.getDistributionRateName();
        if (producerRateString.indexOf('.') < 0) {
            producerRateString = "org.radargun.stressors.producer." + producerRateString;
        }
        try {
            Constructor c = Class.forName(producerRateString).getConstructor(Double.TYPE);
            obj = (ProducerRate) c.newInstance(producerLambda);
        } catch (Exception e) {
            String s = "Could not create ProducerRate of type: " + producerRateString;
            log.error(s);
            throw new RuntimeException(e);
        }
        return obj;
    }

   /**
    * @param producerLambda the lambda (arrival rate) in transaction per millisecond
    */
   protected ProducerRate(double producerLambda) {
      this.lambda = producerLambda;
   }

   /**
    * it sleeps to a determined rate, in order to achieve the lambda (arrival rate) desired
    */
   public final long sleep() {
      //log.trace("Using " + this.getClass().toString());
      long sleepTime = (long) timeToSleep(lambda);
      try {
         Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
          log.warn("Interrupt");
      }
      return sleepTime;
   }

    /**
     * returns the lambda of this producer rate
     *
     * @return the lambda of this producer rate
     */
    public double getLambda() { return lambda; }

    public abstract double timeToSleep(double lambda);


}
