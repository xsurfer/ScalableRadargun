package org.radargun.stages.stressors.systems.workloadGenerators;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.AbstractBenchmarkStage;

/**
 * Generate a workload based on function f(t)=mt+q User: Fabio Perfetti Date: 3/18/13
 */
public class RampWorkloadGenerator extends AbstractWorkloadGenerator {

   private static Log log = LogFactory.getLog(RampWorkloadGenerator.class);

   private double slope = 1.0;

   private double yintercept = 0.0;

   public RampWorkloadGenerator(AbstractBenchmarkStage stage) {
      super(stage);
   }

   @Override
   public int getCurrentArrivalRate() {
      int eval = (int) Math.ceil((slope * getTime()) + yintercept);
      return eval;
   }

   public double getSlope() {
      return this.slope;
   }

   public void setSlope(double slope) {
      this.slope = slope;
   }

   public double getYintercept() {
      return this.yintercept;
   }

   public void setYintercept(double yintercept) {
      this.yintercept = yintercept;
   }

   @Override
   public RampWorkloadGenerator clone() {
      return (RampWorkloadGenerator) super.clone();
   }

}