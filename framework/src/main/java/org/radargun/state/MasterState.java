package org.radargun.state;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.DistStage;
import org.radargun.DistStageAck;
import org.radargun.MasterStage;
import org.radargun.Stage;
import org.radargun.config.DynamicBenchmarkConfig;
import org.radargun.config.FixedSizeBenchmarkConfig;
import org.radargun.config.MasterConfig;
import org.radargun.config.ScalingBenchmarkConfig;
import org.radargun.jmx.JmxRegistration;
import org.radargun.utils.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * State residing on the server, passed to each stage before execution.
 *
 * @author Mircea.Markus@jboss.com
 */
public class MasterState extends StateBase {

   private static Log log = LogFactory.getLog(MasterState.class);

   private MasterConfig config;
   private List<FixedSizeBenchmarkConfig> benchmarks;
   private FixedSizeBenchmarkConfig currentBenchmark;
   private long startTime = System.currentTimeMillis();
   protected DistStage currentDistStage;

   protected ThreadLocal<DistStage> currentDistScalingStageRef = new ThreadLocal<DistStage>();

   private final JmxRegistration jmxRegistration = JmxRegistration.getInstance();

   public MasterState(MasterConfig config) {
      this.config = config;
      benchmarks = new ArrayList<FixedSizeBenchmarkConfig>(config.getBenchmarks());
      if (benchmarks.isEmpty())
         throw new IllegalStateException("There must be at least one benchmark");
      currentBenchmark = benchmarks.remove(0);
      logBenchmarkStarted();
   }

   public MasterConfig getConfig() {
      return config;
   }

    public long getStartTime(){
        return this.startTime;
    }

   public DistStage getNextDistStageToProcess() {
      while (currentBenchmark.hasNextStage()) {
         Stage stage = currentBenchmark.nextStage();
         if (stage instanceof DistStage) {
            currentDistStage = (DistStage) stage;
            return currentDistStage;
         } else {
            jmxRegistration.processStage(stage);
            executeServerStage((MasterStage) stage);
         }
      }
      //if we are here it means we finished executed the current benchmark and we should move to next one
      if (benchmarks.size() == 0) {
         long duration = System.currentTimeMillis() - startTime;
         String duartionStr = Utils.getMillisDurationString(duration);
         log.info("Successfully executed all benchmarks in " + duartionStr + ", exiting.");
         return null;
      }
      currentBenchmark = benchmarks.remove(0);
      logBenchmarkStarted();
      return getNextDistStageToProcess();
   }

    /**
     * This method is useful when you are processing a new slave.<br/>
     * It allows you to execute only the mandatory stages till reaching the currentDistStage, skippable test will be skipped
     * @return
     */
    public DistStage getNextDistScalingStageToProcess() {
        log.trace("getNextDistScalingStageToProcess");
        while (currentBenchmark.hasNextStage()) {
            Stage stage = currentBenchmark.nextStage();
            // finchè non raggiungo il current stage principale
            // seleziono il primo stage da eseguire:
            // se skippable, lo salto
            // altrimenti lo eseguo

            if (stage instanceof DistStage) {
                // se non è uguale allo stato corrente
                if (!(((DistStage) stage).getId().equals(currentDistStage.getId()))) {
                    // allora se è skippabile lo skippo
                    if (((DistStage) stage).isSkippable()) {
                        log.trace("Skipping the stage [id=" + ((DistStage) stage).getId() + "; type=" + stage.getClass().toString() + "]");
                        continue;
                    }
                }
                currentDistScalingStageRef.set((DistStage) stage);
                return currentDistScalingStageRef.get();
            }
            else{
                log.warn("Stage is not an instance of DistStage. Check it. Skipping...");
            }
        }
        return null;
    }

//    @Deprecated
//    public DistStage getNextDistScalingStageToProcess(int slaves) {
//        while (((DynamicBenchmarkConfig) currentBenchmark).hasNextScalingStage(slaves)) {
//            Stage stage = ((DynamicBenchmarkConfig) currentBenchmark).nextScalingStage();
//            if (stage instanceof DistStage) {
//                currentDistScalingStageRef.set((DistStage) stage);
//                return currentDistScalingStageRef.get();
//                //Questo è commentato poichè non ho nessuno scaling stage da eseguire solo sul master
////            } else {
////                jmxRegistration.processStage(stage);
////                executeServerStage((MasterStage) stage);
////            }
//            }
//        }
//
//        // TODO: penso che per il momento è meglio commentare, in questo caso non ci dovrei arrivare poichè muore prima
//        //if we are here it means we finished executed the current benchmark and we should move to next one
////        if (benchmarks.size() == 0) {
////            long duration = System.currentTimeMillis() - startTime;
////            String duartionStr = Utils.getDurationString(duration);
////            log.info("Sono in master state getNextDistScalingStageToProcess, controlla che sta a succede!" +
////                    " Successfully executed all benchmarks in " + duartionStr + ", exiting.");
////            return null;
////        }
////        currentBenchmark = benchmarks.remove(0);
////        logBenchmarkStarted();
//        c
//    }


   public DistStage getCurrentDistStage() {
      return currentDistStage;
   }


    public DistStage getCurrentDistScalingStage() {
        return this.currentDistScalingStageRef.get();
    }

   public int getSlavesCountForCurrentStage() {
      return currentDistStage.getActiveSlaveCount();
   }

    public void setSlavesCountForCurrentStage(int activeSlaves) {
        currentDistStage.setActiveSlavesCount(activeSlaves);
    }

//    public int getSlavesCountForCurrentScalingStage() {
//        log.debug("[[[ MasterState: per questo stage scaling sono attivi " + getCurrentDistScalingStage().getActiveScalingSlavesCount() + "slaves");
//        return getCurrentDistScalingStage().getActiveScalingSlavesCount();
//    }

   public boolean distStageFinished(List<DistStageAck> acks) {
      // Sort acks so that logs are more readable.
      Collections.sort(acks, new Comparator<DistStageAck>() {
         @Override
         public int compare(DistStageAck o1, DistStageAck o2) {
            int thisVal = o1.getSlaveIndex();
            int anotherVal = o2.getSlaveIndex();
            return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
         }
      });
      boolean stageOk = currentDistStage.processAckOnMaster(acks, this);
      if (stageOk) return true;
      if (!currentDistStage.isExitBenchmarkOnSlaveFailure()) {
         log.warn("Execution error for current benchmark, skipping rest of the mandatoryStages");
         currentBenchmark.errorOnCurrentBenchmark();
         return true;
      } else {
         log.info("Exception error on current stage, and exiting (stage's exitBenchmarkOnSlaveFailure is set to true).");
         return false;
      }
   }

    public boolean distScalingStageFinished(List<DistStageAck> acks) {
        boolean stageOk = getCurrentDistScalingStage().processAckOnMaster(acks, this);
        if (stageOk) return true;
        if (!currentDistStage.isExitBenchmarkOnSlaveFailure()) {
            log.warn("Execution error for current benchmark, skipping rest of the mandatoryStages");
            currentBenchmark.errorOnCurrentBenchmark();
            return true;
        } else {
            log.info("Exception error on current stage, and exiting (stage's exitBenchmarkOnSlaveFailure is set to true).");
            return false;
        }
    }

   private void executeServerStage(MasterStage servStage) {
      if (log.isDebugEnabled())
         log.debug("Starting '" + servStage.getClass().getSimpleName() + "' on master node only. Details:" + servStage);
      else
         log.info("Starting '" + servStage.getClass().getSimpleName() + "' on master node only.");
      servStage.init(this);
      try {
         if (!servStage.execute()) {
            log.warn("Issues while executing master stage: " + servStage);
         } else {
            log.trace("Master stage executed successfully " + servStage);
         }
      } catch (Exception e) {
         log.warn("Caught exception", e);
      }
   }

   public String nameOfTheCurrentBenchmark() {
      String prodName = currentBenchmark.getProductName();
      if (prodName == null) {
         throw new IllegalStateException("Null product name not allowed!");
      }
      return prodName;
   }

    public ScalingBenchmarkConfig getCurrentBenchmark() {
        if(currentBenchmark instanceof ScalingBenchmarkConfig)
            return (ScalingBenchmarkConfig) currentBenchmark;
        else
            throw new RuntimeException("currentBenchmark should be a ScalingBenchmarkConfig!");
    }

   public String configNameOfTheCurrentBenchmark() {
      return currentBenchmark.getConfigName();
   }

   private void logBenchmarkStarted() {
      if (currentBenchmark.getProductName() != null) {
         log.info("Started benchmarking product '" + currentBenchmark.getProductName() + "' with configuration '" + currentBenchmark.getConfigName() + "'");
      }
   }
}
