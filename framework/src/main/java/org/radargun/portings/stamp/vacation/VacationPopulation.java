package org.radargun.portings.stamp.vacation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.portings.stamp.vacation.domain.Manager;

public class VacationPopulation {

   private static Log log = LogFactory.getLog(VacationPopulation.class);

   private final CacheWrapper wrapper;
   private int relations;

   public VacationPopulation(CacheWrapper wrapper, int relations) {
      this.wrapper = wrapper;
      this.relations = relations;
   }

   public void performPopulation() {
      int i;
      int t;

      int numRelation = relations;
      int ids[] = new int[numRelation];
      for (i = 0; i < numRelation; i++) {
         ids[i] = i + 1;
      }


      boolean successful = false;
      while (!successful) {
         try {
            wrapper.startTransaction(false);

            Random randomPtr = new Random();
            randomPtr.random_alloc();
            Manager managerPtr = new Manager();

            for (t = 0; t < 4; t++) {

		    /* Shuffle ids */
               for (i = 0; i < numRelation; i++) {
                  int x = randomPtr.posrandom_generate() % numRelation;
                  int y = randomPtr.posrandom_generate() % numRelation;
                  int tmp = ids[x];
                  ids[x] = ids[y];
                  ids[y] = tmp;
               }

		    /* Populate table */
               for (i = 0; i < numRelation; i++) {
                  boolean status = false;
                  int id = ids[i];
                  int num = ((randomPtr.posrandom_generate() % 5) + 1) * 100;
                  int price = ((randomPtr.posrandom_generate() % 5) * 10) + 50;
                  if (t == 0) {
                     status = managerPtr.manager_addCar(wrapper, id, num, price);
                  } else if (t == 1) {
                     status = managerPtr.manager_addFlight(wrapper, id, num, price);
                  } else if (t == 2) {
                     status = managerPtr.manager_addRoom(wrapper, id, num, price);
                  } else if (t == 3) {
                     status = managerPtr.manager_addCustomer(wrapper, id);
                  }
                  assert (status);
               }

            } /* for t */
            wrapper.put(null, "MANAGER", managerPtr);

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
