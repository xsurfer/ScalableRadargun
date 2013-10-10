package org.radargun.stages.stressors.systems.workloadGenerators;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.stages.stressors.systems.RateDistribution;
import org.radargun.utils.Utils;
import org.radargun.utils.WorkerThreadFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created with IntelliJ IDEA.
 * User: Fabio Perfetti
 * Date: 3/18/13
 * Time: 5:10 PM
 *
 */
public abstract class AbstractWorkloadGenerator extends Observable implements Cloneable, Serializable {

    public AbstractWorkloadGenerator(AbstractBenchmarkStage stage){
        this.stage = stage;
    }


    /***************************/
    /***      ATTRIBUTES     ***/
    /***************************/

    private static Log log = LogFactory.getLog(AbstractWorkloadGenerator.class);

    public static final int ARRIVAL_RATE_CHANGED = 2000;

    protected final AbstractBenchmarkStage stage;

    private final String targetDir = "workloads";

    private final String separator = ",";

    private File outputFile;

    private FileWriter fileWriter;

    private List<TimeArrivalRate> outputData = new ArrayList<TimeArrivalRate>();

    private RateDistribution rateDistribution = RateDistribution.EXPONENTIAL;

    /**
     * granularity in milliseconds
     */
    private long granularity = 1000;

    private long firstGranularity;

    /**
     * init time (in seconds)
     */
    private double initTime = 0;

    /**
     * current time (in seconds)
     */
    private volatile double t = 0;

    /**
     * in seconds
     */
    private int maxArrivalRate = -1;

    private AtomicBoolean running = new AtomicBoolean(false);




    /***************************/
    /***    GETTER/SETTER    ***/
    /***************************/

    public RateDistribution getRateDistribution() { return this.rateDistribution; }
    public void setRateDistribution(String rateDistribution) { this.rateDistribution = RateDistribution.valueOf(rateDistribution.toUpperCase());  }

    public long getGranularity() {
       return this.granularity;
    }

    public void setGranularity(long granularityMs) {
       this.granularity = this.firstGranularity = granularityMs;
    }

    public long getFirstGranularity() {
        return this.firstGranularity;
    }

    public void setFirstGranularity(long granularityMs) {
        this.firstGranularity = granularityMs;
    }



    /**
     * returns init time (in seconds)
     */
    public double getInitTime() { return this.initTime; }

    /**
     * Sets the init time
     * @param initTime in ms
     */
    public void setInitTime(double initTime) { this.initTime = initTime; }

    public int getMaxArrivalRate() { return this.maxArrivalRate; }
    public void setMaxArrivalRate(int maxArrivalRate) { this.maxArrivalRate = maxArrivalRate; }

    /**
     * returns current time in seconds
     * @return
     */
    public double getTime(){ return this.t; }

    /***************************/
    /*** TO OVERRIDE METHODS ***/
    /***************************/

    protected abstract int getCurrentArrivalRate();


    /***************************/
    /***       METHODS       ***/
    /***************************/

    protected void prepareOutputFile() {
        File parentDir;
        if (targetDir == null) {
            log.trace("Defaulting to local dir");
            parentDir = new File(".");
        } else {
            parentDir = new File(targetDir);
            if (!parentDir.exists()) {
                if (!parentDir.mkdirs()) {
                    log.warn("Issues creating parent dir " + parentDir);
                }
            }
        }
        assert parentDir.exists() && parentDir.isDirectory();

        String actualFileName = "stage_" +this.stage.getId() + "_" + System.currentTimeMillis() + ".csv";

        try {
            outputFile = Utils.createOrReplaceFile(parentDir, actualFileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeData() {

        try {
            openFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<String> headerRow = new ArrayList<String>();
        headerRow.add("time");
        headerRow.add("arrivalRate");
        try {
            writeRowToFile(headerRow);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<String> dataRow = new ArrayList<String>();
        for (TimeArrivalRate entry : outputData) {
            dataRow.add( Double.toString( entry.time ) );
            dataRow.add( Integer.toString( entry.arrivalRate ) );
            try {
                writeRowToFile(dataRow);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            dataRow.clear();
        }

        try {
            closeFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void closeFile() throws IOException {
        fileWriter.close();
    }

    private void openFile() throws IOException {
        fileWriter = new FileWriter(outputFile);
    }

    private void writeRowToFile(List<String> row) throws IOException {
        for (int i = 0; i < row.size(); i++) {
            fileWriter.write(row.get(i));
            if (i == row.size() - 1) {
                fileWriter.write('\n');
            } else {
                fileWriter.write(separator);
            }
        }
    }



    public void start(){
        if (running.compareAndSet(false, true)) {
            log.trace("Starting workload generator");
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor( new WorkerThreadFactory("WorkloadGenerator", true) );
            executor.execute(new Starter());
            executor.shutdown();
        }
    }

    public void stop(){
        if (running.compareAndSet(true, false)) {
            log.trace("Stopping workload generator");
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.execute(new Stopper());
            executor.shutdown();
        }
    }

    /**
     * Returns the arrival rate.<br/>
     * If maxArrivalRate setted and getCurrentArrivalRate()>maxArrivalRate then maxArrivalRate will be returned
     * @return maxArrivalRate if getCurrentArrivalRate() > maxArrivalRate && maxArrivalRate > 0<br/>
     *  else getCurrentArrivalRate()
     */
    public int arrivalRate(){
        if( getCurrentArrivalRate() < 0 ){
            return 0;
        }
        if( this.maxArrivalRate > 0 && this.getCurrentArrivalRate() > this.maxArrivalRate ){
            log.warn("MaxArrivalRate reached!");
            return getMaxArrivalRate();
        }
        return getCurrentArrivalRate();
    }


    public AbstractWorkloadGenerator clone() {
        try {
            return (AbstractWorkloadGenerator) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }




    /***************************/
    /***    STARTER CLASS    ***/
    /***************************/

    private class Starter implements Runnable{
        @Override
        public void run() {
            BlockingQueue<Double> queue = new LinkedBlockingQueue<Double>();
            t = initTime - ( ( (double) granularity ) /1000 );
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor( new WorkerThreadFactory("TimerIncrementer", true) );
            executor.scheduleAtFixedRate(new TimeIncrementer(queue), firstGranularity, granularity, TimeUnit.MILLISECONDS);
            while (running.get()){
                outputData.add(new TimeArrivalRate(t,getCurrentArrivalRate()));
                setChanged();
                notifyObservers( new Integer(AbstractWorkloadGenerator.ARRIVAL_RATE_CHANGED) );
                try {
                    queue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
            log.info("Workload has been stopped...lets");
            executor.shutdown();

            try {
                executor.awaitTermination(2*granularity, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            prepareOutputFile();
            writeData();
            log.info("Starter died");
        }
    }




    /***************************/
    /***    STOPPER CLASS    ***/
    /***************************/

    private class Stopper implements Runnable{
        @Override
        public void run() {
            log.info("Stopping workload generator");
        }
    }





    /******************************/
    /*** TIME INCREMENTER CLASS ***/
    /******************************/

    private class TimeIncrementer implements Runnable {

        BlockingQueue<Double> queue;

        public TimeIncrementer(BlockingQueue<Double> queue){ this.queue = queue; }

        @Override
        public void run() {
            t+=  ( ( (double) granularity ) /1000 );
            log.trace("Time: "+getTime()+"s, ArrivalRate: " + getCurrentArrivalRate());
            try {
                queue.put(t);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private class TimeArrivalRate {
        private double time;
        private int arrivalRate;

        TimeArrivalRate(double t, int arrivalRate){
            this.time = t;
            this.arrivalRate = arrivalRate;
        }
    }


}