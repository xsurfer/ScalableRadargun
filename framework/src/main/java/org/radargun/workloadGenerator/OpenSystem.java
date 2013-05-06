package org.radargun.workloadGenerator;

import org.radargun.stressors.producer.Producer;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class OpenSystem implements IProducerSystem {

    private AbstractWorkloadGenerator workloadGenerator;

    public AbstractWorkloadGenerator getWorkloadGenerator(){ return workloadGenerator; }

    @Override
    public Producer createProducers() {
        return null;
    }

    @Override
    public String getType() {
        return SystemType.OPEN;
    }
}
