package org.radargun.stages.stressors.systems;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.consumer.Consumer;
import org.radargun.stages.stressors.producer.Producer;
import org.radargun.stages.stressors.systems.workloadGenerators.AbstractWorkloadGenerator;

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

    public AbstractWorkloadGenerator getWorkloadGenerator(){
       return workloadGenerator;
    }

    public void setWorkloadGenerator(AbstractWorkloadGenerator generator) {
       this.workloadGenerator = generator;
    }

    @Override
    public SystemType getType() {
        return SystemType.OPEN;
    }

    @Override
    public Map<String, String> stress(AbstractBenchmarkStressor stressor) {
        return stressor.stress(this);
    }

    @Override
    public void consume(Consumer consumer) {
        consumer.consume(this);
    }

    @Override
    public void finishBenchmark(AbstractBenchmarkStressor stressor) {
        stressor.finishBenchmark(this);
    }

    @Override
    public List<Producer> createProducers(AbstractBenchmarkStressor stressor) {
        return stressor.createProducers(this);
    }


   @Override
   public OpenSystem clone() {
      try {
         OpenSystem clone = (OpenSystem) super.clone();
         clone.workloadGenerator = (AbstractWorkloadGenerator) workloadGenerator.clone();
         return clone;
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException(e);
      }
   }
}
