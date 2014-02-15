package org.radargun.stages.stressors.systems;

import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.consumer.Consumer;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by: Fabio Perfetti E-mail: perfabio87@gmail.com Date: 4/19/13
 */
public interface System extends Serializable, Cloneable {

   public enum SystemType {
      OPEN, CLOSED, MULE;
   }

   public SystemType getType();

   //public Consumer createConsumer(CacheWrapper cacheWrapper, int threadIndex, AbstractBenchmarkStage benchmarkStage, AbstractBenchmarkStressor stressor, Parameters parameters);

   public Map<String, String> stress(AbstractBenchmarkStressor stressor);

   public void consume(Consumer consumer);

   public void finishBenchmark(AbstractBenchmarkStressor stressor);

   public System clone();
}
