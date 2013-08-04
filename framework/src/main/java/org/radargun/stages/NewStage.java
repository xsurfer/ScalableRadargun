package org.radargun.stages;

import org.radargun.stages.stressors.Parameter;
import org.radargun.stages.stressors.StringKeyGenerator;
import org.radargun.stages.stressors.syntethic.SyntheticParameter;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/3/13
 * Time: 2:19 PM
 */
public class NewStage extends AbstractBenchmarkStage<ScalableSyntheticStageStressor, Parameter> {

    /**
     * for each session there will be created fixed number of attributes. On those attributes all the GETs and PUTs are
     * performed (for PUT is overwrite)
     */
    protected int numberOfAttributes = 100;

    /**
     * Each attribute will be a byte[] of this size
     */
    protected int sizeOfAnAttribute = 1000;

    /**
     * Out of the total number of request, this define the frequency of writes (percentage)
     */
    protected int writePercentage = 20;

    protected String keyGeneratorClass = StringKeyGenerator.class.getName();

    private int updateXactWrites = 1;

    private int readOnlyXactSize = 1;

    private int updateXactReads = 1;

    private boolean allowBlindWrites = false;

    private int readsBeforeFirstWrite = 1;


    @Override
    protected Parameter createStressorConfiguration() {
        SyntheticParameter parameters = new SyntheticParameter();

        parameters.setNumberOfAttributes(numberOfAttributes);
        parameters.setNumOfThreads(numOfThreads);
        parameters.setSizeOfAnAttribute(sizeOfAnAttribute);
        parameters.setWritePercentage(writePercentage);
        parameters.setKeyGeneratorClass(keyGeneratorClass);
        parameters.setReadOnlyXactSize(readOnlyXactSize);
        parameters.setUpdateXactReads(updateXactReads);
        parameters.setAllowBlindWrites(allowBlindWrites);
        parameters.setStatsSamplingInterval(statsSamplingInterval);
        parameters.setReadsBeforeFirstWrite(readsBeforeFirstWrite);

        return parameters;
    }

    @Override
    public ScalableSyntheticStageStressor createStressor() {
        return new ScalableSyntheticStageStressor(cacheWrapper, this, system, getStressorParameters());
    }


    public int getNumberOfAttributes() {
        return numberOfAttributes;
    }

    public void setNumberOfAttributes(int numberOfAttributes) {
        this.numberOfAttributes = numberOfAttributes;
    }

    public int getSizeOfAnAttribute() {
        return sizeOfAnAttribute;
    }

    public void setSizeOfAnAttribute(int sizeOfAnAttribute) {
        this.sizeOfAnAttribute = sizeOfAnAttribute;
    }

    public int getWritePercentage() {
        return writePercentage;
    }

    public void setWritePercentage(int writePercentage) {
        this.writePercentage = writePercentage;
    }

    public String getKeyGeneratorClass() {
        return keyGeneratorClass;
    }

    public void setKeyGeneratorClass(String keyGeneratorClass) {
        this.keyGeneratorClass = keyGeneratorClass;
    }

    public int getUpdateXactWrites() {
        return updateXactWrites;
    }

    public void setUpdateXactWrites(int updateXactWrites) {
        this.updateXactWrites = updateXactWrites;
    }

    public int getReadOnlyXactSize() {
        return readOnlyXactSize;
    }

    public void setReadOnlyXactSize(int readOnlyXactSize) {
        this.readOnlyXactSize = readOnlyXactSize;
    }

    public int getUpdateXactReads() {
        return updateXactReads;
    }

    public void setUpdateXactReads(int updateXactReads) {
        this.updateXactReads = updateXactReads;
    }

    public boolean isAllowBlindWrites() {
        return allowBlindWrites;
    }

    public void setAllowBlindWrites(boolean allowBlindWrites) {
        this.allowBlindWrites = allowBlindWrites;
    }

    public int getReadsBeforeFirstWrite() {
        return readsBeforeFirstWrite;
    }

    public void setReadsBeforeFirstWrite(int readsBeforeFirstWrite) {
        this.readsBeforeFirstWrite = readsBeforeFirstWrite;
    }

}
