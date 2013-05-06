package org.radargun.workloadGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stressors.BenchmarkStressor;
import org.radargun.stressors.StressorParameter;
import org.radargun.stressors.consumer.Consumer;
import org.radargun.stressors.consumer.OpenConsumer;
import org.radargun.stressors.producer.GroupProducerRateFactory;
import org.radargun.stressors.producer.OpenProducer;
import org.radargun.stressors.producer.Producer;
import org.radargun.stressors.producer.ProducerRate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class OpenSystem implements IProducerSystem {

    private static Log log = LogFactory.getLog(OpenSystem.class);

    private AbstractWorkloadGenerator workloadGenerator;

    public AbstractWorkloadGenerator getWorkloadGenerator(){ return workloadGenerator; }

    @Override
    public String getType() {
        return SystemType.OPEN;
    }

    @Override
    public Consumer createConsumer(CacheWrapper cacheWrapper, int threadIndex, AbstractBenchmarkStage benchmarkStage, BenchmarkStressor stressor, StressorParameter parameters) {
        Consumer consumer = null;
        consumer = new OpenConsumer(cacheWrapper,
                threadIndex,
                this,
                benchmarkStage,
                stressor,
                parameters
        );
        return consumer;
    }

    @Override
    public Map<String, String> stress(BenchmarkStressor stressor) {
        return stressor.stress(this);
    }

    @Override
    public void finishBenchmark(BenchmarkStressor stressor) {
        stressor.finishBenchmark(this);
    }

    /**
     * Class in charge of create or update the Producer in base of arrival rate.
     *
     */
    public List<Producer> createProducers(CacheWrapper cacheWrapper, AbstractBenchmarkStage benchmarkStage, BenchmarkStressor stressor, StressorParameter parameters) {

        log.info("Creating/Updating producers");

        ProducerRate[] producerRates;
        if (cacheWrapper.isPassiveReplication()) {
            if (cacheWrapper.isTheMaster()) {
                log.info("Creating producers groups for the master. Write transaction percentage is " + benchmarkStage.getWriteWeight());
                producerRates = new GroupProducerRateFactory(getWorkloadGenerator().getRateDistribution(),
                        benchmarkStage.getWriteWeight(),
                        1,
                        parameters.getNodeIndex(),
                        BenchmarkStressor.AVERAGE_PRODUCER_SLEEP_TIME).create();
            } else {
                log.info("Creating producers groups for the slave. Read-only transaction percentage is " + benchmarkStage.getReadWeight());
                producerRates = new GroupProducerRateFactory(getWorkloadGenerator().getRateDistribution(),
                        benchmarkStage.getReadWeight(),
                        cacheWrapper.getNumMembers() - 1,
                        parameters.getNodeIndex() == 0 ? parameters.getNodeIndex() : parameters.getNodeIndex() - 1,
                        BenchmarkStressor.AVERAGE_PRODUCER_SLEEP_TIME).create();
            }
        } else {
            log.info("Creating producers groups");
            producerRates = new GroupProducerRateFactory(getWorkloadGenerator().getRateDistribution(),
                                                         getWorkloadGenerator().getArrivalRate(),
                                                         cacheWrapper.getNumMembers(),
                                                         parameters.getNodeIndex(),
                                                         BenchmarkStressor.AVERAGE_PRODUCER_SLEEP_TIME).create();
        }

        List<Producer> producers = new ArrayList<Producer>();
        for (int i = 0; i < producerRates.length; ++i) {
            producers.add(i, new OpenProducer(stressor, producerRates[i], i));
        }
        return producers;
    }

}
