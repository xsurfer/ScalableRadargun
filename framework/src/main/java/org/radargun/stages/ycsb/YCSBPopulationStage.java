package org.radargun.stages.ycsb;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.stages.AbstractDistStage;
import org.radargun.stages.DefaultDistStageAck;
import org.radargun.portings.ycsb.YCSB;
import org.radargun.stressors.ycsb.YCSBPopulationStressor;

public class YCSBPopulationStage extends AbstractDistStage {

   private int recordCount;

   public void setRecordCount(int recordCount) {
      this.recordCount = recordCount;
   }

   @Override
   public DistStageAck executeOnSlave() {
      YCSB.preinit();
      DefaultDistStageAck ack = newDefaultStageAck( this.getClass().getName() );
      CacheWrapper wrapper = slaveState.getCacheWrapper();
      if (wrapper == null) {
         log.info("Not executing any test as the wrapper is not set up on this slave ");
         return ack;
      }
      if (!wrapper.isCoordinator()) {
         log.info("Skipping population, delegating to the coordinator");
      } else {
         long startTime = System.currentTimeMillis();
         populate(wrapper);
         long duration = System.currentTimeMillis() - startTime;
         log.info("The population took: " + (duration / 1000) + " seconds.");
         ack.setPayload(duration);
      }
      return ack;
   }

   private void populate(CacheWrapper wrapper) {
      YCSBPopulationStressor ycsbStressor = new YCSBPopulationStressor();
      ycsbStressor.setRecordCount(recordCount);
      ycsbStressor.stress(wrapper);
   }
}
