package org.radargun.workloadGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stressors.producer.ClosedProducer;
import org.radargun.stressors.producer.GroupProducerRateFactory;
import org.radargun.stressors.producer.Producer;
import org.radargun.stressors.producer.ProducerRate;
import org.radargun.stages.AbstractBenchmarkStage;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 3/18/13
 */

// todo da eliminare!!

@Deprecated
public class ClosedWorkloadGenerator extends AbstractWorkloadGenerator {

    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */


    private static Log log = LogFactory.getLog(ClosedWorkloadGenerator.class);


    /* ****************** */
    /* *** CONSTRUCTOR *** */
    /* ****************** */

    public ClosedWorkloadGenerator(AbstractBenchmarkStage stage) {
        super(stage);
    }

    /* *************** */
    /* *** METHODS *** */
    /* *************** */

    public List<Producer> createProducers(CacheWrapper cacheWrapper,int nodeIndex, double writeWeight, double readWeight) {
        List<Producer> producers = new ArrayList<Producer>();

        final int THINK_TIME = 5000;
        ProducerRate[] producerRates;
        if (cacheWrapper.isPassiveReplication()) {
            if (cacheWrapper.isTheMaster()) {
                log.info("Creating producers groups for the master. Write transaction percentage is " + writeWeight);
                producerRates = new GroupProducerRateFactory(getRateDistribution(),
                        writeWeight,
                        1,
                        nodeIndex,
                        THINK_TIME).create();
            } else {
                log.info("Creating producers groups for the slave. Read-only transaction percentage is " + readWeight);
                producerRates = new GroupProducerRateFactory(getRateDistribution(),
                        readWeight,
                        cacheWrapper.getNumMembers() - 1,
                        nodeIndex == 0 ? nodeIndex : nodeIndex - 1,
                        THINK_TIME).create();
            }
        } else {
            log.info("Creating producers groups");
            producerRates = GroupProducerRateFactory.createClients( getPopulation(),
                                                                    getRateDistribution(),
                                                                    cacheWrapper.getNumMembers(),
                                                                    nodeIndex,
                                                                    getThinkTime());
        }
        //producers = new Producer[producerRates.length];

        producers.clear();
        for (int i = 0; i < producerRates.length; ++i) {
            producers.add(i, new ClosedProducer(stage.getStressor(), getThinkTime(), nodeIndex) );
            //producers[i] = new Producer(producerRates[i], i);
        }


        return producers;

    }



    /* ****************** */
    /* *** OVERRIDING *** */
    /* ****************** */

    @Override
    public int getCurrentArrivalRate() {
        return 0;
    }

    @Override
    public ClosedWorkloadGenerator clone(){
        return (ClosedWorkloadGenerator) super.clone();
    }

}
