package org.radargun.stages.stressors.syntethic;

import org.radargun.CacheWrapper;
import org.radargun.stages.stressors.Parameters;
import org.radargun.stages.stressors.StringKeyGenerator;
import org.radargun.stages.synthetic.XACT_RETRY;

import java.util.concurrent.CountDownLatch;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com)
 * Date: 8/3/13
 * Time: 2:40 PM
 */
public class SyntheticParameters extends Parameters {

    /**
     * for each session there will be created fixed number of attributes. On those attributes all the GETs and PUTs are
     * performed (for PUT is overwrite)
     */
    protected final int numberOfAttributes;

    /**
     * Each attribute will be a byte[] of this size
     */
    protected final int sizeOfAnAttribute;

    /**
     * Out of the total number of request, this define the frequency of writes (percentage)
     */
    protected int writePercentage;

    protected final String keyGeneratorClass;

    private int updateXactWrites;

    private int updateXactReads;

    private int readOnlyXactSize;

    private final boolean allowBlindWrites;

    private final int readsBeforeFirstWrite;

    public SyntheticParameters(CacheWrapper cacheWrapper,
                               long simulationTimeSec,
                               int numOfThreads,
                               int nodeIndex,
                               long backOffTime,
                               XACT_RETRY retryOnAbort,
                               long statsSamplingInterval,

                               int numberOfAttributes,
                               int sizeOfAnAttribute,
                               int writePercentage,
                               String keyGeneratorClass,
                               int updateXactWrites,
                               int readOnlyXactSize,
                               int updateXactReads,
                               boolean allowBlindWrites,
                               int readsBeforeFirstWrite
                               ) {
        super(cacheWrapper, simulationTimeSec, numOfThreads, nodeIndex, backOffTime, retryOnAbort, statsSamplingInterval);
        this.numberOfAttributes = numberOfAttributes;
        this.sizeOfAnAttribute = sizeOfAnAttribute;
        this.writePercentage = writePercentage;
        this.keyGeneratorClass = keyGeneratorClass;
        this.updateXactWrites = updateXactWrites;
        this.updateXactReads = updateXactReads;
        this.readOnlyXactSize = readOnlyXactSize;
        this.allowBlindWrites = allowBlindWrites;
        this.readsBeforeFirstWrite = readsBeforeFirstWrite;

    }


    public int getNumberOfAttributes() {
        if(numberOfAttributes == -1){
            throw new RuntimeException("Never update setting!");
        }
        return numberOfAttributes;
    }

//    public void setNumberOfAttributes(int numberOfAttributes) {
//        this.numberOfAttributes = numberOfAttributes;
//    }

    public int getSizeOfAnAttribute() {
        if(sizeOfAnAttribute == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
        return sizeOfAnAttribute;
    }

//    public void setSizeOfAnAttribute(int sizeOfAnAttribute) {
//        this.sizeOfAnAttribute = sizeOfAnAttribute;
//    }

    public int getWritePercentage() {
        if(writePercentage == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
        return writePercentage;
    }

    public void setWritePercentage(int writePercentage) {
        this.writePercentage = writePercentage;
    }

    public String getKeyGeneratorClass() {
        if(keyGeneratorClass == null){
            throw new IllegalArgumentException("Never update setting!");
        }
        return keyGeneratorClass;
    }

//    public void setKeyGeneratorClass(String keyGeneratorClass) {
//        this.keyGeneratorClass = keyGeneratorClass;
//    }

    public int getUpdateXactWrites() {
        if(updateXactWrites == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
        return updateXactWrites;
    }

    public void setUpdateXactWrites(int updateXactWrites) {
        this.updateXactWrites = updateXactWrites;
    }

    public int getUpdateXactReads() {
        if(updateXactReads == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
        return updateXactReads;
    }

    public void setUpdateXactReads(int updateXactReads) {
        this.updateXactReads = updateXactReads;
    }

    public int getReadOnlyXactSize() {
        if(readOnlyXactSize == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
        return readOnlyXactSize;
    }

    public void setReadOnlyXactSize(int readOnlyXactSize) {
        this.readOnlyXactSize = readOnlyXactSize;
    }

    public boolean isAllowBlindWrites() {
        return allowBlindWrites;
    }

//    public void setAllowBlindWrites(boolean allowBlindWrites) {
//        this.allowBlindWrites = allowBlindWrites;
//    }

    public int getReadsBeforeFirstWrite() {
        if(readsBeforeFirstWrite == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
        return readsBeforeFirstWrite;
    }

//    public void setReadsBeforeFirstWrite(int readsBeforeFirstWrite) {
//        this.readsBeforeFirstWrite = readsBeforeFirstWrite;
//    }

}