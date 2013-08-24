package org.radargun.stages.stressors.syntethic;

import org.radargun.stages.stressors.Parameters;
import org.radargun.stages.stressors.StringKeyGenerator;

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
    protected int numberOfAttributes = -1;

    /**
     * Each attribute will be a byte[] of this size
     */
    protected int sizeOfAnAttribute = -1;

    /**
     * Out of the total number of request, this define the frequency of writes (percentage)
     */
    protected int writePercentage = -1;

    protected String keyGeneratorClass;

    private int updateXactWrites = -1;

    private int readOnlyXactSize = -1;

    private int updateXactReads = -1;

    private boolean allowBlindWrites = false;

    private int readsBeforeFirstWrite = -1;



    public int getNumberOfAttributes() {
        if(numberOfAttributes == -1){
            throw new RuntimeException("Never update setting!");
        }
        return numberOfAttributes;
    }

    public void setNumberOfAttributes(int numberOfAttributes) {
        this.numberOfAttributes = numberOfAttributes;
    }

    public int getSizeOfAnAttribute() {
        if(sizeOfAnAttribute == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
        return sizeOfAnAttribute;
    }

    public void setSizeOfAnAttribute(int sizeOfAnAttribute) {
        this.sizeOfAnAttribute = sizeOfAnAttribute;
    }

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

    public void setKeyGeneratorClass(String keyGeneratorClass) {
        this.keyGeneratorClass = keyGeneratorClass;
    }

    public int getUpdateXactWrites() {
        if(updateXactWrites == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
        return updateXactWrites;
    }

    public void setUpdateXactWrites(int updateXactWrites) {
        this.updateXactWrites = updateXactWrites;
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

    public int getUpdateXactReads() {
        if(updateXactReads == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
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
        if(readsBeforeFirstWrite == -1){
            throw new IllegalArgumentException("Never update setting!");
        }
        return readsBeforeFirstWrite;
    }

    public void setReadsBeforeFirstWrite(int readsBeforeFirstWrite) {
        this.readsBeforeFirstWrite = readsBeforeFirstWrite;
    }
}
