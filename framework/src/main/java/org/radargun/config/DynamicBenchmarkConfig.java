package org.radargun.config;

/**
 * Created with IntelliJ IDEA.
 * User: fabio
 * Date: 12/5/12
 * Time: 11:59 AM
 * To change this template use File | Settings | File Templates.
 */
/**
 * A fixed size benchmark is a benchmark that executes over a fixed number of slaves. This defines the configuration of
 * such a benchmark.
 * <p/>
 * Ho aggiunto una scalingStagesRef che è una variabile locale al thread poichè ogni thread deve portare avanti un suo stato
 *
 * @author Mircea.Markus@jboss.com
 * @see org.radargun.config.ScalingBenchmarkConfig
 */

@Deprecated
public class DynamicBenchmarkConfig extends FixedSizeBenchmarkConfig {
//
//    private static Log log = LogFactory.getLog(DynamicBenchmarkConfig.class);
//
//
//    /*******************************************/
//    /************ AGGIUNTI DA FABIO*************/
//    /*******************************************/
//    private boolean initialized = false;
//
//    /** questa variabile non deve essere utilizzata poichè contiene lo stack originale che sarà poi copiato da altri thread **/
//    protected List<Stage> scalingStagesOrig = new ArrayList<Stage>();
//
//    protected ThreadLocal<Integer> scalingSize = new ThreadLocal<Integer>();
//
//    protected ThreadLocal<Integer> stScalingIterator = new ThreadLocal<Integer>() {
//        protected Integer initialValue() {
//            return new Integer(0);
//        }
//    };
//
//    private ThreadLocal<List<Stage>> scalingStagesRef = new ThreadLocal<List<Stage>>() {
//        protected List<Stage> initialValue() {
//            return new ArrayList<Stage>(scalingStagesOrig);
//        }
//    };
//
//    /********************************************/
//
//
//    public DynamicBenchmarkConfig() {
//    }
//
//
//    public void setScalingStages(List<Stage> stages) {
//        scalingStagesRef.set(new ArrayList<Stage>(stages));
//    }
//
//    public void addScalingStage(Stage stage) {
//        //scalingStagesRef.get().add(stage);
//        this.scalingStagesOrig.add(stage);
//    }
//
//    public List<Stage> getScalingStages() {
//        return new ArrayList<Stage>(this.scalingStagesRef.get());
//    }
//
//
//    @Override
//    public DynamicBenchmarkConfig clone() {
//        log.debug("DynamicBenchmarkConfig CLONING");
//        DynamicBenchmarkConfig clone = (DynamicBenchmarkConfig) super.clone();
//        clone.scalingStagesOrig = cloneStages(this.scalingStagesOrig);
//        return clone;
//    }
//
//    public boolean hasNextStage() {
//        initialize();
//        return stIterator < stages.size();
//    }
//
//    public boolean hasNextScalingStage(int slaves) {
//        if(scalingSize.get()==null){
//            scalingSize.set(new Integer(slaves));
//        }
//        return stScalingIterator.get() < getScalingStages().size();
//    }
//
//    private void initialize() {
//        if (!initialized) {
//            this.setSize(getMaxSize());
//        }
//        initialized = true;
//    }
//
//
//    public Stage nextScalingStage() {
//        Stage stage = scalingStagesRef.get().get(stScalingIterator.get());
//        int stScalingIteratorInt = stScalingIterator.get().intValue();
//        stScalingIteratorInt++;
//        stScalingIterator.set(stScalingIteratorInt);
//        if (stage instanceof DistStage) {
//            DistStage distStage = (DistStage) stage;
//            // TODO: dacci 'na controllata qui sotto
//            if (!distStage.isRunOnAllSlaves()) {
//                distStage.setActiveScalingSlavesCount(scalingSize.get());
//                distStage.setActiveSlavesCount(size);
//            } //else {
//            //    if (maxSize <= 0) throw new IllegalStateException("Make sure you set the maxSize first!");
//            //    distStage.setActiveSlavesCount(maxSize);
//            //}
//        }
//        return stage;
//    }
//
//    protected List<Stage> cloneScalingStages(List<Stage> stages) {
//        List<Stage> clone = new ArrayList<Stage>();
//        for (Stage st : this.getScalingStages()) {
//            clone.add(st.clone());
//        }
//        return clone;
//    }
}
