package org.radargun.state;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.*;
import org.radargun.config.FixedSizeBenchmarkConfig;
import org.radargun.config.MasterConfig;
import org.radargun.config.ScalingBenchmarkConfig;
import org.radargun.jmx.JmxRegistration;
import org.radargun.stages.AbstractBenchmarkStage;
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

    //protected DistStage currentMainDistStage;

    protected volatile int sizeForNextStage = -1;

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

                if(currentDistStage instanceof AbstractBenchmarkStage){
                    ((AbstractBenchmarkStage) currentDistStage).setInitTimeStamp();
                }

                // eliminato nel refactoring del 2/08/2013
//                if(this.sizeForNextStage<0){
//                    log.info("Initializing sizeForNextStage to: " + ((DistStage) stage).getActiveSlaveCount());
//                    this.sizeForNextStage = ((DistStage) stage).getActiveSlaveCount();
//                }
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

    public DistStage getCurrentDistStage() {
        return currentDistStage;
    }

    public int getSlavesCountForCurrentStage() {
        return currentDistStage.getActiveSlaveCount();
    }

    public void setSlavesCountForCurrentStage(int activeSlaves) {
        currentDistStage.setActiveSlavesCount(activeSlaves);
    }

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

        if (stageOk){
            return true;
        }
        if (!currentDistStage.isExitBenchmarkOnSlaveFailure()) {
            log.warn("Execution error for current benchmark, skipping rest of the stages");
            currentBenchmark.errorOnCurrentBenchmark();
            return true;
        } else {
            log.info("Exception error on current stage, and exiting (stage's exitBenchmarkOnSlaveFailure is set to true).");
            return false;
        }
    }

    public List<Integer> sizeForNextStage(List<DistStageAck> acks, List<SlaveSocketChannel> slaves) {
        List<Integer> ret = currentDistStage.sizeForNextStage(acks, slaves);
        this.sizeForNextStage = slaves.size() - ret.size();
        return ret;
    }

    public int getSizeForNextStage(){
        return sizeForNextStage;
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

//    /**
//     * This method MUST BE USED ONLY by the ClusterExecutor thread
//     * @param currentMainDistStage
//     */
//    public void setCurrentMainDistStage(DistStage currentMainDistStage) {
//        log.info("Setting CurrentMainDistStage to: [ id = " + currentMainDistStage.getId() + "]" );
//        this.currentMainDistStage = currentMainDistStage;
//    }
//
//    public DistStage getCurrentMainDistStage() { return this.currentMainDistStage; }
}
