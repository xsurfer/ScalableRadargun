package org.radargun.stages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.stressors.microbenchmark.MicrobenchmarkPopulationStressor;

public class MicrobenchmarkPopulationStage extends AbstractDistStage {

    private static Log log = LogFactory.getLog(MicrobenchmarkPopulationStage.class);

    private int items;
    private int range;
    private String set;
    
    @Override
    public DistStageAck executeOnSlave() {
        DefaultDistStageAck ack = newDefaultStageAck( this.getClass().getName() );
        CacheWrapper wrapper = slaveState.getCacheWrapper();
        if (wrapper == null) {
            log.info("Not executing any test as the wrapper is not set up on this slave ");
            return ack;
        }
        long startTime = System.currentTimeMillis();
        populate(wrapper);
        long duration = System.currentTimeMillis() - startTime;
        log.info("The population took: " + (duration / 1000) + " seconds.");
        ack.setPayload(duration);
        return ack;
    }

    private void populate(CacheWrapper wrapper) {
        MicrobenchmarkPopulationStressor stressor = new MicrobenchmarkPopulationStressor();
        stressor.setItems(items);
        stressor.setRange(range);
        stressor.setSet(set);
        stressor.stress(wrapper);
    }

    public int getItems() {
        return items;
    }

    public void setItems(int items) {
        this.items = items;
    }

    public int getRange() {
        return range;
    }

    public void setRange(int range) {
        this.range = range;
    }

    public String getSet() {
        return set;
    }

    public void setSet(String set) {
        this.set = set;
    }
    
}
