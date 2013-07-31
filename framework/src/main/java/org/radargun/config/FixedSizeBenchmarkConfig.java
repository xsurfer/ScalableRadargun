package org.radargun.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.DistStage;
import org.radargun.Stage;
import org.radargun.utils.TypedProperties;
import org.radargun.utils.Utils;

import java.util.ArrayList;
import java.util.List;

/**
 * A fixed size benchmark is a benchmark that executes over a fixed number of slaves. This defines the configuration of
 * such a benchmark.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.radargun.config.ScalingBenchmarkConfig
 */
public class FixedSizeBenchmarkConfig implements Cloneable {

    private static Log log = LogFactory.getLog(FixedSizeBenchmarkConfig.class);

    /**
     * originalStackStages contains the original stack stage.
     */
    protected List<Stage> originalStackStages = new ArrayList<Stage>();

    public void addOriginalStage(Stage stage) { originalStackStages.add(stage); }
    public List<Stage> getOriginalStages() { return new ArrayList<Stage>(originalStackStages); }
    public void setOriginalStages(List<Stage> stages) { this.originalStackStages = new ArrayList<Stage>(stages); }

    /**
     *
     */
    protected ThreadLocal<List<Stage>> stages = new ThreadLocal<List<Stage>>() {
        protected List<Stage> initialValue() {
            return new ArrayList<Stage>(originalStackStages);
        }
    };

    //protected List<Stage> stages = new ArrayList<Stage>();

    protected String productName;
    protected String configName;

    /**
     * Current number of slaves for this test.<br>
     */
    protected int size;

    private TypedProperties configAttributes;


    //protected int stIterator = 0;
    protected ThreadLocal<Integer> stIterator = new ThreadLocal<Integer>() {
        protected Integer initialValue() {
            return new Integer(0);
        }
    };

    /**
     * Mandatory max number of slaves
     */
    private int maxSize = -1;

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public FixedSizeBenchmarkConfig() {
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        assertNo_(productName);
        this.productName = productName;
    }

    private void assertNo_(String name) {
        if (name.indexOf("_") >= 0) {
            throw new RuntimeException("'_' not allowed in productName (reporting relies on that)");
        }
    }

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        configName = Utils.fileName2Config(configName);
        assertNo_(configName);
        this.configName = configName;
    }

    public void setConfigAttributes(TypedProperties typedProperties) {
        this.configAttributes = typedProperties;
    }

    public TypedProperties getConfigAttributes() {
        return configAttributes;
    }


    public void validate() {
        if (productName == null) throw new RuntimeException("Name must be set!");
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public FixedSizeBenchmarkConfig clone() {
        FixedSizeBenchmarkConfig clone;
        try {
            clone = (FixedSizeBenchmarkConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Impossible!!!");
        }
        clone.stages.set(cloneStages(this.stages.get()));
        return clone;
    }

    /**
     * Returns true if there is a next stage to execute <br>
     * ATTENTION: the stage stack is saved using a ThreadLocal
     * @return Stage
     */
    public boolean hasNextStage() {
        return stIterator.get() < stages.get().size();
    }

    /**
     * Returns next stage<br>
     * ATTENTION: the stage stack is saved using a ThreadLocal
     * @return Stage
     */
    public Stage nextStage() {
        Stage stage = stages.get().get(stIterator.get());
        stIterator.set(stIterator.get()+1);
        if (stage instanceof DistStage) {
            DistStage distStage = (DistStage) stage;
            if (!distStage.isRunOnAllSlaves()) {
                //log.debug("size for this fixed benchmark is: " + size);
                distStage.setActiveSlavesCount(size);
            } else {
                if (maxSize <= 0) throw new IllegalStateException("Make sure you set the maxSize first!");
                distStage.setActiveSlavesCount(maxSize);
            }
        }
        return stage;
    }

    public void errorOnCurrentBenchmark() {
        log.trace("Issues in current benchmark, skipping remaining stages");
        stIterator.set(stages.get().size());
    }

    protected List<Stage> cloneStages(List<Stage> stages) {
        List<Stage> clone = new ArrayList<Stage>();
        for (Stage st : stages) {
            clone.add(st.clone());
        }
        return clone;
    }
}
