package org.radargun.stressors.producer;

import org.radargun.workloadGenerator.AbstractWorkloadGenerator;

/**
 * Classes that know how to create producers at the desire rate
 *
 * @author Pedro Ruivo
 * @author Diego Didona
 * @since 1.1
 */
public class GroupProducerRateFactory {
   private final double originalLambda; //tx/sec
   private final int numberOfNodes;
   private final int nodeIndex;
   private final int avgSleepTime;
   private final AbstractWorkloadGenerator.RateDistribution rateDistribution;

   /**
    * @param rateDistribution the rateDistribution type (exponential, uniform)
    * @param globalLambda  the global system lambda (a.k.a arrival rate) in transactions per seconds
    * @param numberOfNodes the number of nodes in the system (>= 1)
    * @param nodeIndex     the node index [0..numberOfNodes - 1]
    * @param avgSleepTime  The average sleeping time desire for a producer
    */
   public GroupProducerRateFactory(AbstractWorkloadGenerator.RateDistribution rateDistribution,
                                   double globalLambda, int numberOfNodes, int nodeIndex, int avgSleepTime) {
      if (numberOfNodes < 1) {
         throw new IllegalArgumentException("Number of nodes must be higher or equals than 1");
      }
      if (nodeIndex < 0 || nodeIndex >= numberOfNodes) {
         throw new IllegalArgumentException("The node index is not valid");
      }
      this.originalLambda = globalLambda;
      this.numberOfNodes = numberOfNodes;
      this.nodeIndex = nodeIndex;
      this.avgSleepTime = avgSleepTime;
      this.rateDistribution = rateDistribution;
   }

   /**
    * it creates an array of producers, each one with the desire rate in order to achieve the global system rate
    *
    * @return an array of producers
    */
   public final ProducerRate[] create() {
      double remainder = originalLambda % numberOfNodes;

      //this is the producer rate common to all nodes
      double myLambda = (originalLambda - remainder) / numberOfNodes;

      //if this node is unlucky, it can get more load than the others
      if (nodeIndex < remainder) {
         myLambda++;
      }

      myLambda /= 1000D;

      //calculate the number of producers needed
      double numberOfProducers = myLambda * avgSleepTime;

      //the number of producers at Normal producer rate
      int numberOfNormalProducers = (int) Math.floor(numberOfProducers);

      double normalProducerRate = 1D / avgSleepTime;

      //it is possible to have a producer that works more slowly than the others
      double slowProducerRate = myLambda - (numberOfNormalProducers * normalProducerRate);

      ProducerRate[] producers = new ProducerRate[numberOfNormalProducers + (slowProducerRate != 0 ? 1 : 0)];

      for (int i = 0; i < numberOfNormalProducers; ++i) {
          try {
              producers[i] = ProducerRate.createInstance(rateDistribution, normalProducerRate);
          } catch (ProducerRate.ProducerRateException e) {
              throw new RuntimeException(e);
          }
      }

      //the slower producer
      if (slowProducerRate != 0) {
          try {
              producers[producers.length - 1] = ProducerRate.createInstance(rateDistribution, slowProducerRate);
          } catch (ProducerRate.ProducerRateException e) {
              throw new RuntimeException(e);
          }
      }
      return producers;
   }


    /**
     * it creates an array of producers, each one with the desire rate in order to achieve the global system rate
     *
     * @return an array of producers
     */
    public static ProducerRate[] createClients(int populationSize,
                                                     AbstractWorkloadGenerator.RateDistribution rateDistribution,
                                                     int numberOfNodes,
                                                     int nodeIndex,
                                                     long thinkTime ) {

        int remainder = populationSize % numberOfNodes;

        //this is the producer rate common to all nodes
        int myClients = (int) (populationSize - remainder) / numberOfNodes;

        //if this node is unlucky, it can get more load than the others
        if (nodeIndex < remainder) {
            myClients++;
        }

        ProducerRate[] producers = new ProducerRate[myClients];

        for (int i = 0; i < myClients; ++i) {
            try {
                producers[i] = ProducerRate.createInstance(rateDistribution, thinkTime);
            } catch (ProducerRate.ProducerRateException e) {
                throw new RuntimeException(e);
            }
        }


        return producers;
    }
}
