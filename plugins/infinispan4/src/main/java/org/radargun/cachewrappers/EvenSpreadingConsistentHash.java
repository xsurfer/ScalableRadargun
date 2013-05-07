package org.radargun.cachewrappers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.radargun.stages.stressors.ObjectKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 */
public class EvenSpreadingConsistentHash /* implements ConsistentHash*/ {

   private static Log log = LogFactory.getLog(EvenSpreadingConsistentHash.class);

   /**
    * Why static? because the consistent hash is recreated when cluster changes and there's no other way to pass these
    * across
    */
   private volatile static int threadCountPerNode = -1;
   private volatile static int keysPerThread = -1;
   private volatile DefaultConsistentHash existing;

   private final List<Address> cachesList = new ArrayList<Address>();


   public EvenSpreadingConsistentHash() {//needed for UT
      existing = new DefaultConsistentHash();
   }


    public List<Integer> getHashIds(Address a) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


   public List<Address> locate(Object key, int replCount) {
      throw new RuntimeException("Distribution not supported yet");
      /*
      if(! (key instanceof ObjectKey)) {
         if (log.isTraceEnabled()) log.trace("Delegating key " + key + " to default CH");
         return existing.locate(key, replCount);
      }

      if (threadCountPerNode <= 0 || keysPerThread <= 0) throw new IllegalStateException("keysPerThread and threadCountPerNode need to be set!");


      Set<Address> caches = existing.getCaches();
      int clusterSize = caches.size();

      int keyIndexInCluster = getSequenceNumber((ObjectKey) key);
      int firstIndex = keyIndexInCluster % caches.size();

      List<Address> result = new ArrayList<Address>();

      List<Address> addresses = Immutables.immutableListConvert(caches);
      for (int i = 0; i < replCount; i++) {
         Address address = cachesList.get((firstIndex + i) % clusterSize);
         result.add(address);
         if (result.size() == replCount) break;
      }
      if (log.isTraceEnabled())
         log.trace("Handling key " + key + ", clusterIndex==" + keyIndexInCluster +" and EvenSpreadingConsistentHash --> " + result);

      return Collections.unmodifiableList(result);
      */
   }

   private int getSequenceNumber(ObjectKey key) {
      return key.getKeyIndexInCluster(threadCountPerNode, keysPerThread);
   }

   public void init(int threadCountPerNode, int keysPerThread) {
      log.trace("Setting threadCountPerNode =" + threadCountPerNode + " and keysPerThread = " + keysPerThread);
      this.threadCountPerNode = threadCountPerNode;
      this.keysPerThread = keysPerThread;
   }

   //following methods should only be used during rehashing, so no point in implementing them



    public Set<Address> getCaches() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


   public List<Address> getStateProvidersOnLeave(Address leaver, int replCount) {
      return existing.getStateProvidersOnLeave(leaver, replCount);
   }


   public List<Address> getStateProvidersOnJoin(Address joiner, int replCount) {
      return existing.getStateProvidersOnJoin(joiner, replCount);
   }

   public List<Address> getBackupsForNode(Address node, int replCount) {
      if (log.isTraceEnabled()) log.trace("getBackupsForNode (" + node +")");
      return existing.getBackupsForNode(node, replCount);
   }

   //@Override
   public Address primaryLocation(Object key) {
      throw new RuntimeException("Distribution not supported yet");
         /*
      return existing.primaryLocation(key);
      */
   }

   //@Override
   public void setCaches(Set<Address> caches) {
      throw new RuntimeException("Distribution not supported yet");
            /*
      existing.setCaches(caches);

      cachesList.addAll(caches);
      Collections.sort(cachesList, new Comparator<Address>() {
         @Override
         public int compare(Address o1, Address o2) {
            return o1.toString().compareTo(o2.toString());
         }
      });
      */
   }

   public Map<Object, List<Address>> locateAll(Collection<Object> keys, int replCount) {
      Map<Object, List<Address>> locations = new HashMap<Object, List<Address>>();
      for (Object k : keys) locations.put(k, locate(k, replCount));
      return locations;
   }

   public boolean isKeyLocalToAddress(Address a, Object key, int replCount) {
      // simple, brute-force impl
      return locate(key, replCount).contains(a);
   }

   /*
   @Override
   public List<Integer> getHashIds(Address a) {
      return existing.getHashIds(a);
   }

   @Override
   public Set<Address> getCaches() {
      return existing.getCaches();
   }
   */




   public int getHashId(Address a) {
      throw new RuntimeException("Distribution not supported yet");
      //return 0;  //To change body of implemented methods use File | Settings | File Templates.
   }


   public int getHashSpace() {
      throw new RuntimeException("Distribution not supported yet");
      //return 0;  //To change body of implemented methods use File | Settings | File Templates.
   }


   public void setCaches(List<Address> caches) {
      throw new RuntimeException("Distribution not supported yet");
      //To change body of implemented methods use File | Settings | File Templates.
   }


   /*@Override
   public ReplicationGroup getGroupFor(Object key, int replicationCount) {
      return null;  // TODO: Customise this generated block
   }*/
}
