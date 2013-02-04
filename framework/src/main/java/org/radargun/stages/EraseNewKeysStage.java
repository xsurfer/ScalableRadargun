package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;

import static org.radargun.utils.Utils.printMemoryFootprint;

/**
 * This stage erases the
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 19/12/12
 */
public class EraseNewKeysStage extends AbstractDistStage {

   private int batchSize = 50;


   @Override
   public DistStageAck executeOnSlave() {
      DefaultDistStageAck defaultDistStageAck = newDefaultStageAck(this.getClass().getName());
      CacheWrapper cacheWrapper = slaveState.getCacheWrapper();

      if (cacheWrapper == null) {
         log.info("Not erasing new keys on this slave as the wrapper hasn't been configured.");
         return defaultDistStageAck;
      }


      log.info(printMemoryFootprint(true));
      long start = System.currentTimeMillis();
      cacheWrapper.eraseNewKeys(this.batchSize);
      long duration = System.currentTimeMillis() - start;
      log.info(printMemoryFootprint(false));
      defaultDistStageAck.setDuration(duration);

      log.info("Skipping garbage collection");
      /*
      start = System.currentTimeMillis();
      System.gc();
      duration = System.currentTimeMillis() - start;
      log.info("Garbage collection took "+duration+" msec");
      log.info(printMemoryFootprint(false));
      */
      return defaultDistStageAck;
   }

   public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
   }

   @Override
   public String toString() {
      return "EraseNewKeysStage{" +
              "batchSize=" + batchSize +
              " " + super.toString() +
              '}';
   }
}
