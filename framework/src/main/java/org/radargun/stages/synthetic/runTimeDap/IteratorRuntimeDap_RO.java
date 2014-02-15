package org.radargun.stages.synthetic.runTimeDap;

import org.radargun.stages.stressors.KeyGenerator;
import org.radargun.stages.synthetic.SyntheticParameters;
import org.radargun.stages.synthetic.XactOp;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */

public class IteratorRuntimeDap_RO implements Iterator<XactOp> {


   private SyntheticParameters params;
   private int toRead;
   private KeyGenerator keyGen;
   private Random r;
   private int numKeys;
   private int nodeIndex;
   private int threadIndex;
   private Set<Integer> readSet;

   private int current = 0;

   @Override
   public boolean hasNext() {
      return current < toRead;
   }

   @Override
   public XactOp next() {
      boolean okRead = false;
      Object key;
      int keyToAccess;
      do {
         keyToAccess = r.nextInt(numKeys);
         if (!readSet.contains(keyToAccess))
            okRead = true;
      }
      while (!okRead);

      key = keyGen.generateKey(nodeIndex, threadIndex, keyToAccess);
      readSet.add(keyToAccess);
      current++;
      return new XactOp(key, "", false);
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
   }

   public IteratorRuntimeDap_RO(SyntheticParameters params, KeyGenerator keyGenerator, Random random, int threadIndex) {
      this.params = params;
      toRead = params.getReadOnlyXactSize();
      keyGen = keyGenerator;
      r = random;
      numKeys = params.getNumberOfAttributes();
      nodeIndex = params.getNodeIndex();
      this.threadIndex = threadIndex;
      readSet = new HashSet<Integer>();
   }
}
