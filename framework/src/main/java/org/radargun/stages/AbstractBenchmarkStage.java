package org.radargun.stages;

import org.radargun.DistStage;
import org.radargun.DistStageAck;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.state.MasterState;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 3/23/13
 */
public abstract class AbstractBenchmarkStage extends AbstractDistStage {

    /***************************/
    /***      ATTRIBUTES     ***/
    /***************************/

    /**
     * total time (in seconds) of simulation for each stressor thread
     */
    protected long perThreadSimulTime = 180L;

    /**
     * timestamps (in ms) when this stage was starting
     */
    private long initTimeStamp = 0L;

    /**
     * the workload generator
     */
    AbstractWorkloadGenerator workloadGenerator;



    /***************************/
    /***    GETTER/SETTER    ***/
    /***************************/

    public void setWorkloadGenerator(AbstractWorkloadGenerator wg){ this.workloadGenerator = wg; }
    public AbstractWorkloadGenerator getWorkloadGenerator(){ return this.workloadGenerator; }

    public long getPerThreadSimulTime(){ return this.perThreadSimulTime; }
    public void setPerThreadSimulTime(long perThreadSimulTime){ this.perThreadSimulTime = perThreadSimulTime; }

    public long getInitTimeStamp() { return this.initTimeStamp; }
    public void setInitTimeStamp() { this.initTimeStamp = System.currentTimeMillis(); log.info("SETTING initTimeStamp to: " + initTimeStamp); }



    /***************************/
    /*** TO OVERRIDE METHODS ***/
    /***************************/

    @ManagedOperation(description = "Stop the current benchmark")
    public abstract void stopBenchmark();





    /***************************/
    /***       METHODS       ***/
    /***************************/

    public void initOnMaster(MasterState masterState, int slaveIndex) {
        super.initOnMaster(masterState, slaveIndex);
        this.setInitTimeStamp();
    }


    public void updateTimes(DistStage currentMainStage) {

        log.info("Updating perThreadSimulTime");

        long totalSimulTime = ((AbstractBenchmarkStage) currentMainStage).getPerThreadSimulTime();
        long currentMainStageInitTs = ((AbstractBenchmarkStage) currentMainStage).getInitTimeStamp();
        long toExecuteInitTs = this.getInitTimeStamp();
        long elapsedTimeFromBeginning = toExecuteInitTs - currentMainStageInitTs;
        long secondToExecute = totalSimulTime - (elapsedTimeFromBeginning / 1000);
        if (secondToExecute < 0) { secondToExecute = 0; }

        log.info("This stage will execute for: " + secondToExecute);
        this.setPerThreadSimulTime(secondToExecute);

        log.info("Updating initTime Workload Generator");

        this.getWorkloadGenerator().setInitTime( (int) (elapsedTimeFromBeginning / 1000) );
    }


    /**
     * This method iterates acks list looking for nodes stopped by JMX.<br/>
     * @param acks Acks from previous stage
     * @param slaves All the slaves actually running the test
     * @return List of slaveIndex stopped by JMX
     */
    public List<Integer> sizeForNextStage(List<DistStageAck> acks, List<SocketChannel> slaves){
        List<Integer> ret = new ArrayList<Integer>();

        if(acks.size() != slaves.size())
            throw new IllegalStateException("Number of acks and number of slaves MUST be ugual");

        for (DistStageAck ack : acks) {
            DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
            Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
            if (benchResult != null) {
                Object stoppedByJmxObj = benchResult.get("STOPPED");
                if (stoppedByJmxObj == null) {
                    throw new IllegalStateException("STOPPED should be there!");
                }
                boolean stoppedByJmx = Boolean.parseBoolean(stoppedByJmxObj.toString());
                if(stoppedByJmx){
                    log.info("Slave " + ack.getSlaveIndex() + " has been stopped via JMX, I don't use it anymore");
                    ret.add(new Integer(ack.getSlaveIndex()));
                }
            } else {
                log.warn("No report received from slave: " + ack.getSlaveIndex());
            }
        }

        log.info("Next stage will start with: " + (slaves.size() - ret.size()) + " slaves");
        return ret;
    }

    public AbstractBenchmarkStage clone() {
        AbstractBenchmarkStage clone = (AbstractBenchmarkStage) super.clone();
        log.info("cloning AbstractBenchmarkStage");
        clone.initTimeStamp = 0;
        clone.workloadGenerator = workloadGenerator.clone();

        return clone;
    }
}
