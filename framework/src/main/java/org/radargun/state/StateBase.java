package org.radargun.state;

import java.util.HashMap;
import java.util.Map;

/**
 * Support class for master and slave states.
 *
 * @author Mircea.Markus@jboss.com
 */
public class StateBase {
   private Map<Object, Object> stateMap = new HashMap<Object, Object>();

   public void put(Object key, Object value) {
      stateMap.put(key, value);
   }

   public Object get(String key) {
      return stateMap.get(key);
   }

   public String getString(Object key) {
      return (String) stateMap.get(key);
   }

   public void remove(Object key) {
      stateMap.remove(key);
   }

   public Integer getInteger(Object key) {
      Object value = stateMap.get(key);
      if (value == null) return null;
      if (value instanceof Integer) {
         return (Integer) value;
      } else {
         return Integer.parseInt(value.toString());
      }
   }

   public Float getFloat(Object key) {
      Object value = stateMap.get(key);
      if (value == null) return null;
      if (value instanceof Float) {
         return (Float) value;
      } else {
         return Float.parseFloat(value.toString());
      }
   }

   public Boolean getBoolean(Object key) {
      Object value = stateMap.get(key);
      if (value == null) return null;
      if (value instanceof Boolean) {
         return (Boolean) value;
      } else {
         return Boolean.parseBoolean(value.toString());
      }
   }
}
