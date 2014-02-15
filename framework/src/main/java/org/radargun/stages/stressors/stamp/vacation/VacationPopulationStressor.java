package org.radargun.stages.stressors.stamp.vacation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.portings.stamp.vacation.VacationPopulation;
import org.radargun.stages.stressors.AbstractCacheWrapperStressor;

import java.util.Map;

public class VacationPopulationStressor extends AbstractCacheWrapperStressor {

   private static Log log = LogFactory.getLog(VacationPopulationStressor.class);

   private int RELATIONS;

   public void setRelations(int RELATIONS) {
      this.RELATIONS = RELATIONS;
   }

   @Override
   public Map<String, String> stress(CacheWrapper wrapper) {
      if (wrapper == null) {
         throw new IllegalStateException("Null wrapper not allowed");
      }
      if (!wrapper.isCoordinator()) {
         log.info("Skipping Population Operations, delegate to the coordinator");
      } else {
         try {
            log.info("Performing Population Operations");
            new VacationPopulation(wrapper, RELATIONS).performPopulation();
         } catch (Exception e) {
            log.warn("Received exception during cache population" + e.getMessage());
            e.printStackTrace();
         }
      }
      return null;
   }

   @Override
   public void destroy() throws Exception {
      //Don't destroy data in cache!
   }

}
