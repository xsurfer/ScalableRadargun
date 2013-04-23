package org.radargun.workloadGenerator;

import org.radargun.stressors.producer.Producer;

import java.lang.*;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public interface IProducerSystem extends System {

    public Producer createProducers();

}
