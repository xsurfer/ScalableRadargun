package org.radargun.stages;

import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedAttribute;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.stages.stressors.SyntheticStressor;
import org.radargun.stages.synthetic.SyntheticParameters;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/5/13 Time: 11:03 AM
 */
@MBean(objectName = "SyntheticBenchmark", description = "Synthetic benchmark stage that generates synthetic workload")
public class SyntheticBenchmarkStage extends AbstractBenchmarkStage<SyntheticStressor, SyntheticParameters> {

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

   //If >=0, it is the number of reads to perform before doing the first put
   private int readsBeforeFirstWrite = -1;   //-1 means do not take into account this params

   //If >0, after each operation the xact will spin on the cpu for a time proportional to this value (to simulate cpu operations)
   private long spinBetweenOps = 0L;


   @Override
   protected SyntheticParameters createStressorConfiguration() {
      log.trace("Creating SyntheticParameters...");

      SyntheticParameters parameters = new SyntheticParameters(
            cacheWrapper,
            simulationTimeSec,
            numOfThreads,
            getSlaveIndex(),
            backOffTime,
            retryOnAbort,
            statsSamplingInterval,
            numberOfAttributes, sizeOfAnAttribute,
            writePercentage,
            keyGeneratorClass,
            updateXactWrites,
            readOnlyXactSize,
            updateXactReads,
            allowBlindWrites,
            readsBeforeFirstWrite,
            spinBetweenOps
      );
      return parameters;
   }

   @Override
   public SyntheticStressor createStressor() {
      log.trace("Creating SyntheticStressor...");
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
      if (numberOfAttributes == -1) {
         throw new IllegalArgumentException("Never update setting!");
      }
      log.trace("numberOfAttributes: " + numberOfAttributes);
      return numberOfAttributes;
   }

   public void setNumberOfAttributes(int numberOfAttributes) {
      log.trace("Set numberOfAttributes to : " + numberOfAttributes);
      this.numberOfAttributes = numberOfAttributes;
   }

   public int getSizeOfAnAttribute() {
      if (sizeOfAnAttribute == -1) {
         throw new IllegalArgumentException("Never update setting!");
      }
      log.trace("sizeOfAnAttribute: " + sizeOfAnAttribute);
      return sizeOfAnAttribute;
   }

   public void setSizeOfAnAttribute(int sizeOfAnAttribute) {
      log.trace("Set sizeOfAnAttribute to : " + sizeOfAnAttribute);
      this.sizeOfAnAttribute = sizeOfAnAttribute;
   }

   public int getWritePercentage() {
      if (writePercentage == -1) {
         throw new IllegalArgumentException("Never update setting!");
      }
      log.trace("writePercentage: " + writePercentage);
      return writePercentage;
   }

   public void setWritePercentage(int writePercentage) {
      log.trace("Set writePercentage to : " + writePercentage);
      this.writePercentage = writePercentage;
   }

   public String getKeyGeneratorClass() {
      if (keyGeneratorClass == null) {
         throw new IllegalArgumentException("Never update setting!");
      }
      log.trace("keyGeneratorClass: " + keyGeneratorClass);
      return keyGeneratorClass;
   }

   public void setKeyGeneratorClass(String keyGeneratorClass) {
      log.trace("Set keyGeneratorClass to : " + keyGeneratorClass);
      this.keyGeneratorClass = keyGeneratorClass;
   }

   public int getUpdateXactWrites() {
      if (updateXactWrites == -1) {
         throw new IllegalArgumentException("Never update setting!");
      }
      log.trace("updateXactWrites: " + updateXactWrites);
      return updateXactWrites;
   }

   public void setUpdateXactWrites(int updateXactWrites) {
      log.trace("Set updateXactWrites to : " + updateXactWrites);
      this.updateXactWrites = updateXactWrites;
   }

   public int getReadOnlyXactSize() {
      if (readOnlyXactSize == -1) {
         throw new IllegalArgumentException("Never update setting!");
      }
      log.trace("readOnlyXactSize: " + readOnlyXactSize);
      return readOnlyXactSize;
   }

   public void setReadOnlyXactSize(int readOnlyXactSize) {
      log.trace("Set readOnlyXactSize to : " + readOnlyXactSize);
      this.readOnlyXactSize = readOnlyXactSize;
   }

   public int getUpdateXactReads() {
      if (updateXactReads == -1) {
         throw new IllegalArgumentException("Never update setting!");
      }
      log.trace("updateXactReads: " + updateXactReads);
      return updateXactReads;
   }

   public void setUpdateXactReads(int updateXactReads) {
      log.trace("Set updateXactReads to : " + updateXactReads);
      this.updateXactReads = updateXactReads;
   }

   public boolean isAllowBlindWrites() {
      log.trace("allowBlindWrites: " + allowBlindWrites);
      return allowBlindWrites;
   }

   public void setAllowBlindWrites(boolean allowBlindWrites) {
      log.trace("Set allowBlindWrites to : " + allowBlindWrites);
      this.allowBlindWrites = allowBlindWrites;
   }

   public int getReadsBeforeFirstWrite() {
      if (readsBeforeFirstWrite == -1) {
         throw new IllegalArgumentException("Never update setting!");
      }
      log.trace("readsBeforeFirstWrite: " + readsBeforeFirstWrite);
      return readsBeforeFirstWrite;
   }

   public void setReadsBeforeFirstWrite(int readsBeforeFirstWrite) {
      log.trace("Set setReadsBeforeFirstWrite to : " + readsBeforeFirstWrite);
      this.readsBeforeFirstWrite = readsBeforeFirstWrite;
   }

   public void setSpinBetweenOps(long spinBetweenOps) {
      this.spinBetweenOps = spinBetweenOps;
   }
}
