package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.state.MasterState;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/1/13 Time: 6:44 PM
 */
public class SupportExecutor extends AbstractExecutor {

   private static Log log = LogFactory.getLog(SupportExecutor.class);

   private ClusterExecutor clusterExecutor;

   private AtomicBoolean stopped = new AtomicBoolean(false);

   public SupportExecutor(MasterState state, Set<SlaveSocketChannel> slaves, ClusterExecutor clusterExecutor) {
      super(state, slaves);
      this.clusterExecutor = clusterExecutor;
   }


   protected void prepareNextStage() throws Exception {

      DistStage toExecute = null;

      // salta finchè non è raggiunto il current stage.
      // nel caso in cui lo stage non è skippable ma è settato come RunOnAllSlaves allora lancia una eccezione
      // se lo stage non è skippable e non è RunOnAllSlaves allora devo eseguirlo

      boolean isCurrent = false;
      do {
         toExecute = state.getNextDistStageToProcess();
         if (toExecute == null) {
            releaseResourcesAndExit();
            //ShutDownHook.exit(0);
         } else {
            isCurrent = toExecute.getId().equals(clusterExecutor.currentDistStage().getId());
         }

         if (!toExecute.isSkippable() && toExecute.isRunOnAllSlaves()) {
            throw new IllegalStateException("While adding a slave at runtime, stage " + toExecute.getId() + " cannot be executed on all slaves. If you can't change it, please set it as skippable.");
         }

         if (isCurrent) {
            log.trace("Reachead Cluster Current Dist Stage");
         } else if (toExecute.isSkippable()) {
            log.trace("Skipping the stage [id=" + toExecute.getId() + "; type=" + toExecute.getClass().toString() + "]");
         }


      } while (!isCurrent && toExecute.isSkippable());

      // using slaves.size it will send next stage only to new slaves
      runDistStage(toExecute);
   }

   @Override
   protected int numAckToNextStage() {
      return slaves.size();
   }

   @Override
   protected final void slaveStopped(SlaveSocketChannel slave) {
      log.fatal("A new slave ended!!!");
      clusterExecutor.slaveStopped(slave);
   }

   @Override
   protected boolean assertRunning() {
      return !stopped.get();
   }

   protected void preSerialization(DistStage readyToWriteOnBuffer) {
      if (readyToWriteOnBuffer instanceof AbstractBenchmarkStage) {
         ((AbstractBenchmarkStage) readyToWriteOnBuffer).updateTimes(clusterExecutor.getInitTsCurrentStage());
      }
   }

   @Override
   protected void post() {
      for (SlaveSocketChannel scc : slaves) {
         clusterExecutor.mergeSlave(scc);
      }
      log.info("Sent data to master, killing!");
   }

   protected void postStageBroadcast() {
      // checking if main current stage has been reached
      if (state.getCurrentDistStage().getId().equals(clusterExecutor.currentDistStage().getId())) {
         log.info("CurrentMainStage reached, preparing to quit...");
         stopped.compareAndSet(false, true);
      }
   }

}