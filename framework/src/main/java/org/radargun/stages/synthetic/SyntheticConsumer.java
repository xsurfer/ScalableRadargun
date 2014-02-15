package org.radargun.stages.synthetic;

import org.radargun.CacheWrapper;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.consumer.Consumer;
import org.radargun.stages.stressors.systems.System;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/3/13 Time: 3:47 PM
 */
public class SyntheticConsumer extends Consumer {

   private SyntheticXactFactory factory;

   public SyntheticConsumer(CacheWrapper cacheWrapper,
                            int threadIndex,
                            System system,
                            AbstractBenchmarkStage stage,
                            AbstractBenchmarkStressor stressor,
                            SyntheticParameters parameters,
                            SyntheticXactFactory factory) {
      super(cacheWrapper, threadIndex, system, stage, stressor, parameters, factory);


      this.factory = factory;
   }

   public SyntheticXactFactory getFactory() {
      return factory;
   }

}
