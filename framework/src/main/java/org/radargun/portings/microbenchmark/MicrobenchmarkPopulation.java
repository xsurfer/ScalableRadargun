package org.radargun.portings.microbenchmark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.portings.microbenchmark.domain.IntSet;
import org.radargun.portings.microbenchmark.domain.IntSetLinkedList;
import org.radargun.portings.microbenchmark.domain.IntSetRBTree;
import org.radargun.portings.microbenchmark.domain.IntSetSkipList;
import org.radargun.portings.microbenchmark.domain.IntSetTreeMap;

import java.util.Random;

public class MicrobenchmarkPopulation {

   private static Log log = LogFactory.getLog(MicrobenchmarkPopulation.class);

   private final CacheWrapper wrapper;
   private final int items;
   private final int range;
   private final String set;

   public MicrobenchmarkPopulation(CacheWrapper wrapper, int items, int range, String set) {
      this.wrapper = wrapper;
      this.items = items;
      this.range = range;
      this.set = set;
   }

   public void performPopulation() {

      Random random = new Random();
      boolean successful = false;
      while (!successful) {
         try {
            wrapper.startTransaction(false);

            IntSet mySet = null;
            if (set.equals("ll")) {
               mySet = new IntSetLinkedList(wrapper);
            } else if (set.equals("sl")) {
               mySet = new IntSetSkipList(wrapper);
            } else if (set.equals("rb")) {
               mySet = new IntSetRBTree(wrapper);
            } else if (set.equals("tm")) {
               mySet = new IntSetTreeMap(wrapper);
            }

            for (int i = 0; i < items; i++)
               mySet.add(wrapper, random.nextInt(range));

            wrapper.put(null, "SET", mySet);

            wrapper.endTransaction(true);
            successful = true;
         } catch (Throwable e) {
            System.out.println("Exception during population, going to rollback after this");
            e.printStackTrace();
            log.warn(e);
            try {
               wrapper.endTransaction(false);
            } catch (Throwable e2) {
               System.out.println("Exception during rollback!");
               e2.printStackTrace();
            }
         }
      }

      System.gc();
   }

}
