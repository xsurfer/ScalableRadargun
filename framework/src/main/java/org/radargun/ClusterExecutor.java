package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.state.MasterState;

import javax.management.MalformedObjectNameException;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/1/13 Time: 2:47 PM
 */
public class ClusterExecutor extends AbstractExecutor {

   private static Log log = LogFactory.getLog(ClusterExecutor.class);

   private Set<SlaveSocketChannel> joiningSlaves = new HashSet<SlaveSocketChannel>();

   private Set<SlaveSocketChannel> stoppedSlaves = new HashSet<SlaveSocketChannel>();

   /**
    * timestamp at the beginning of the current stage
    */
   private long initTsCurrentStage = 0L;


   public ClusterExecutor(MasterState state, Set<SlaveSocketChannel> slaves) {
      super(state, slaves);
   }

   @Override
   protected void slaveStopped(SlaveSocketChannel slave) {
      int newSize = state.getCurrentDistStage().getActiveSlaveCount() - 1;

      log.info("Editing state.getCurrentMainDistStage().getActiveSlaveCount(): from " + state.getCurrentDistStage().getActiveSlaveCount() + " to " + newSize);
      state.getCurrentDistStage().setActiveSlavesCount(newSize);

      log.info("Editing state.getCurrentBenchmark().currentFixedBenchmark().getSize: from " + state.getCurrentBenchmark().currentFixedBenchmark().getSize() + " to " + newSize);
      state.getCurrentBenchmark().currentFixedBenchmark().setSize(newSize);

      if (!slaves.remove(slave)) {
         if (joiningSlaves.remove(slave)) {
            log.info("Joining slave deleted");
         } else {
            throw new RuntimeException("Slave " + slave.getId() + " doesn't belong to any set!!");
         }
      } else {
         log.info("Slave deleted");
         stoppedSlaves.add(slave);
      }

      log.info("Unregistering the socketChannel " + slave.getSocketChannel());
      SelectionKey key = slave.getSocketChannel().keyFor(communicationSelector);
      key.cancel();

      log.info("Number of working slaves: " + slaves.size());
      log.info("Number of joining slaves: " + joiningSlaves.size());
      log.info("Number of stopped slaves: " + stoppedSlaves.size());

      changeNumSlavesNotify();
   }

   public void addSlave(SlaveSocketChannel slavesReadyToMerge) {

      int newSize = state.getCurrentDistStage().getActiveSlaveCount() + 1;

      log.debug("Editing state.getCurrentMainDistStage().getActiveSlaveCount(): from " + state.getCurrentDistStage().getActiveSlaveCount() + " to " + newSize);
      state.getCurrentDistStage().setActiveSlavesCount(newSize);

      log.debug("Editing state.getCurrentBenchmark().currentFixedBenchmark().getSize: from " + state.getCurrentBenchmark().currentFixedBenchmark().getSize() + " to " + newSize);
      state.getCurrentBenchmark().currentFixedBenchmark().setSize(newSize);

      joiningSlaves.add(slavesReadyToMerge);
   }

   public void mergeSlave(SlaveSocketChannel slaveReadyToMerge) {

      if (joiningSlaves.contains(slaveReadyToMerge)) {
         // ************************** This code should be executed atomically *******************
         log.trace("Merging slave id: " + slaveReadyToMerge.getId());
         slaves.add(slaveReadyToMerge);
         joiningSlaves.remove(slaveReadyToMerge);
         initBuffer(slaveReadyToMerge.getSocketChannel());
         // ************************** This code should be executed atomically *******************

      } else {
         log.error("Trying to merge a not joining slave!!!");
         throw new RuntimeException("Trying to merge a not joining slave!!!");
      }

      log.trace("slave id: " + slaveReadyToMerge.getId() + " merged");
      communicationSelector.wakeup();

      changeNumSlavesNotify();
   }

   private void changeNumSlavesNotify() {
      log.info("Notifying all the slaves (" + slaves.size() + ") to change numNodes");

      for (SlaveSocketChannel slave : slaves) {
         String host = slave.getSocketChannel().socket().getInetAddress().getHostName();
         String currentBenchmark = state.getCurrentDistStage().getClass().getName();

         int startIndex = currentBenchmark.lastIndexOf(".") + 1;
         int lastIndex = currentBenchmark.length() - new String("Stage").length();
         currentBenchmark = currentBenchmark.substring(startIndex, lastIndex);

         log.trace("CurrentStage: " + currentBenchmark);

         NumNodesJmxRequest jmxRequest = null;

         try {
            jmxRequest = new NumNodesJmxRequest(currentBenchmark, host, NumNodesJmxRequest.DEFAULT_JMX_PORT);
         } catch (IOException e) {
            if (log.isTraceEnabled()) {
               log.trace(e, e);
            } else {
               log.info("An error occured while contacting node " + host + ". Enable log trace to print all the stack trace. Skipping...");
            }
            continue;
         } catch (MalformedObjectNameException e) {
            if (log.isTraceEnabled()) {
               log.trace(e, e);
            } else {
               log.info("An error occured while contacting node " + host + ". Enable log trace to print all the stack trace. Skipping...");
            }
            continue;
         }

         log.info("Notifying slave " + slave.getId());
         jmxRequest.doRequest();
      }
   }


   public long getInitTsCurrentStage() {
      return initTsCurrentStage;
   }

   public void setInitTsCurrentStage(long initTsCurrentStage) {
      this.initTsCurrentStage = initTsCurrentStage;
   }

   @Override
   protected void preSerialization(DistStage readyToWriteOnBuffer) {
      setInitTsCurrentStage(System.currentTimeMillis());
   }

   @Override
   protected void post() {
      // nothing to do
   }

   public DistStage currentDistStage() {
      return state.getCurrentDistStage();
   }

   @Override
   protected boolean assertRunning() {
      return true;    // cluster executor always run, till the end of the benchmark
   }

   @Override
   protected void postStageBroadcast() {
      stoppedSlaves.clear();
   }

   @Override
   protected synchronized int numAckToNextStage() {
      return slaves.size() + joiningSlaves.size() + stoppedSlaves.size();
   }


}
