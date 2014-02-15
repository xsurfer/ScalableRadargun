package org.radargun.stages.synthetic.initTimeDap;

import org.radargun.ITransaction;
import org.radargun.stages.synthetic.SyntheticParameters;
import org.radargun.stages.synthetic.SyntheticXact;
import org.radargun.stages.synthetic.SyntheticXactFactory;
import org.radargun.stages.synthetic.XactOp;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticDistinctXactFactory_PreDaP extends SyntheticXactFactory {


   public SyntheticDistinctXactFactory_PreDaP(SyntheticParameters parameters, int threadIndex) {
      super(parameters, threadIndex);

   }


   //TODO: we can consider moving the generation of the read/write set in the iterator
   //in this way runtimeXact and preXact differ only in the buildIterator method
   //and in both cases the cost of choosing data items is *inside* tx scope and not outside (thus not traced)

   @Override
   protected ITransaction generateROXact() {
      SyntheticXact xact = new SyntheticXact(true);
      xact.setSpinBetweenOps(parameters.getSpinBetweenOps());
      xact.setIterator(new IteratorPreDap(this.buildReadSet()));
      return xact;
   }

   @Override
   protected ITransaction generateUPXact() {
      SyntheticXact xact = new SyntheticXact(false);
      xact.setSpinBetweenOps(parameters.getSpinBetweenOps());
      xact.setIterator(new IteratorPreDap(this.buildReadWriteSet()));
      return xact;
   }

   private XactOp[] buildReadSet() {
      int numR = parameters.getReadOnlyXactSize();
      XactOp[] ops = new XactOp[numR];
      Object key;
      int keyToAccess;
      int numKeys = parameters.getNumberOfAttributes();
      int nodeIndex = parameters.getNodeIndex();
      Set<Integer> alreadyRead = new HashSet<Integer>();
      boolean okRead;
      for (int i = 0; i < numR; i++) {
         okRead = false;
         do {
            keyToAccess = rnd.nextInt(numKeys);
            if (!alreadyRead.contains(keyToAccess))
               okRead = true;
         }
         while (!okRead);

         key = keyGenerator.generateKey(nodeIndex, threadIndex, keyToAccess);
         alreadyRead.add(keyToAccess);
         ops[i] = new XactOp(key, "", false);
      }
      return ops;
   }

   private XactOp[] buildReadWriteSet() {
      XactOp[] ops = new XactOp[rwB.length];
      List<Integer> readSet = new ArrayList<Integer>(), writeSet = new ArrayList<Integer>();
      int numReads = parameters.getUpdateXactReads();
      int numWrites = parameters.getUpdateXactWrites();
      int total = numReads + numWrites;
      int nodeIndex = parameters.getNodeIndex();
      int sizeS = parameters.getSizeOfAnAttribute();
      boolean bW = parameters.isAllowBlindWrites();
      int numK = parameters.getNumberOfAttributes();
      Integer key;
      int nextWrite = 0; //without blind writes, this points to the next read item to write. The first operation is a read
      //Generate rwSet
      try {
         for (int i = 0; i < total; i++) {
            if (!rwB[i]) {  //Read
               do {
                  key = rnd.nextInt(numK);
               }
               while (readSet.contains(key));  //avoid repetitions
               readSet.add(0, key);
               ops[i] = new XactOp(keyGenerator.generateKey(nodeIndex, threadIndex, key),
                                   null, false);    //add a read op and increment
            } else {    //Put
               if (bW) {        //You can have (distinct) blind writes
                  do {
                     key = rnd.nextInt(numK);
                  }
                  while (writeSet.contains(key));  //avoid repetitions among writes
                  writeSet.add(0, key);
                  ops[i] = new XactOp(keyGenerator.generateKey(nodeIndex, threadIndex, key),
                                      generateRandomString(sizeS), true);    //add a write op
               } else { //No blind writes: Take a value already read and increment         To have distinct writes, remember numWrites<=numReads in this case
                  ops[i] = new XactOp(ops[nextWrite++].getKey(),
                                      generateRandomString(sizeS), true);

                  while (nextWrite < total && rwB[nextWrite]) {       //scan the bitmask up to the next read operation while it is a put op, go on
                     nextWrite++;
                  }
               }
            }
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
      if (log.isTraceEnabled())
         log.trace(ops);
      return ops;
   }

}
