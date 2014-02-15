package org.radargun.stages.stressors.systems;

import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.producer.Producer;

import java.util.List;

/**
 * Created by: Fabio Perfetti E-mail: perfabio87@gmail.com Date: 5/6/13
 */
public abstract interface IProducerSystem<T extends Producer> extends System {

   public List<T> createProducers(AbstractBenchmarkStressor stressor);

}
