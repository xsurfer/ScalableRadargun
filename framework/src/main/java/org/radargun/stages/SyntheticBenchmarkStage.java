package org.radargun.stages;

import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedAttribute;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.stages.stressors.StringKeyGenerator;
import org.radargun.stages.stressors.SyntheticStressor;
import org.radargun.stages.stressors.syntethic.SyntheticParameters;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/5/13
 * Time: 11:03 AM
 */
@MBean(objectName = "SyntheticBenchmark", description = "Synthetic benchmark stage that generates synthetic workload")
public class SyntheticBenchmarkStage extends AbstractBenchmarkStage<SyntheticStressor, SyntheticParameters> {

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
    protected SyntheticParameters createStressorConfiguration() {
        SyntheticParameters parameters = new SyntheticParameters();
        parameters.setNumberOfAttributes( getNumberOfAttributes() );
        parameters.setSizeOfAnAttribute( getSizeOfAnAttribute() );
        parameters.setWritePercentage( getWritePercentage() );
        parameters.setKeyGeneratorClass( getKeyGeneratorClass() );
        parameters.setUpdateXactWrites( getUpdateXactWrites() );
        parameters.setUpdateXactReads( getUpdateXactReads() );
        parameters.setAllowBlindWrites( isAllowBlindWrites() );
        parameters.setReadsBeforeFirstWrite( getReadsBeforeFirstWrite() );

        return parameters;
    }

    @Override
    public SyntheticStressor createStressor() {
        return new SyntheticStressor(cacheWrapper, this, system, getStressorParameters());
    }


    /* ******************* */
    /* *** JMX METHODS *** */
    /* ******************* */

    @ManagedOperation(description = "Change the update tx, percentage and size")
    public void changeUpdateTx(int writePercentage, int updateXactReads, int updateXactWrites) {
        stressor.changeUpdateTx(writePercentage, updateXactReads, updateXactWrites);
    }

    @ManagedOperation(description = "Change the readonly tx, percentage and size")
    public void changeReadOnlyTx(int readOnlyPercentage, int readOnlyXactSize) {
        stressor.changeReadOnlyTx(readOnlyPercentage, readOnlyXactSize);
    }

    @ManagedAttribute(description = "Returns the Write Tx percentage", writable = false)
    public final double getWriteWeight() {
        return stressor.getWriteWeight();
    }

    @ManagedAttribute(description = "Returns the Read Tx percentage", writable = false)
    public final double getReadWeight() {
        return stressor.getReadWeight();
    }



    /* **************************** */
    /* ***    GET/SET METHODS   *** */
    /* *** used with reflection *** */
    /* **************************** */

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
