package org.radargun.workloadGenerator;

import org.radargun.stressors.producer.Producer;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class ClosedSystem implements IProducerSystem {

    private int population = 0;

    private long thinkTime = 0;

    @Override
    public Producer createProducers() {
        return null;
    }

    @Override
    public String getType() {
        return SystemType.CLOSED;
    }

    public long getThinkTime(){ return thinkTime; }
    public void setThinkTime(long val){ thinkTime=val; }

    public long getPopulation(){ return population; }
    public void setPopulation(int val){ population = val; }

}
