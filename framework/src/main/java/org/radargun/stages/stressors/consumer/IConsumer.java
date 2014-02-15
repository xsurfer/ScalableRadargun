package org.radargun.stages.stressors.consumer;

import org.radargun.stages.stressors.producer.Producer;
import org.radargun.stages.stressors.systems.ClosedSystem;
import org.radargun.stages.stressors.systems.MuleSystem;
import org.radargun.stages.stressors.systems.OpenSystem;

/**
 * Created with IntelliJ IDEA. User: fabio Date: 7/25/13 Time: 1:06 PM To change this template use File | Settings |
 * File Templates.
 */
public interface IConsumer extends Runnable {

   public void consume(OpenSystem system);

   public void consume(ClosedSystem system);

   public void consume(MuleSystem system);

   public void notifyProducer(Producer producer);

   public void disable();

   public void enable();

   public boolean isEnabled();


}
