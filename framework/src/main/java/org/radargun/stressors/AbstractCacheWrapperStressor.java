package org.radargun.stressors;

import org.radargun.CacheWrapperStressor;

import java.util.Observer;

/**
 * @author Mircea Markus <mircea.markus@gmail.com>
 */
public abstract class AbstractCacheWrapperStressor implements Observer, CacheWrapperStressor {

   private boolean sysMonitorEnabled = false;

   @Override
   public void setSysMonitorEnabled(boolean enabled) {
      sysMonitorEnabled = enabled;
   }

   @Override
   public boolean isSysMonitorEnabled() {
      return sysMonitorEnabled;
   }
}
