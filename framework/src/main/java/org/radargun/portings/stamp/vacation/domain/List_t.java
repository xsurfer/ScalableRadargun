package org.radargun.portings.stamp.vacation.domain;

import org.radargun.CacheWrapper;
import org.radargun.portings.stamp.vacation.Cons;
import org.radargun.portings.stamp.vacation.Vacation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class List_t<E> implements Serializable {

   protected /* final */ String cacheKey;

   public List_t() {
   }

   public List_t(CacheWrapper cache, String strKey) {
      this.cacheKey = UUID.randomUUID().toString() + ":" + strKey;
      Vacation.put(cache, this.cacheKey, (Cons<E>) Cons.empty());
   }

   private void putElements(CacheWrapper cache, Cons<E> elems) {
      Vacation.put(cache, this.cacheKey, elems);
   }

   private Cons<E> getElements(CacheWrapper cache) {
      return ((Cons<E>) Vacation.get(cache, this.cacheKey));
   }

   public void add(CacheWrapper cache, E element) {
      putElements(cache, getElements(cache).cons(element));
   }

   public E find(CacheWrapper cache, int type, int id) {
      for (E iter : getElements(cache)) {
         if (iter instanceof Reservation_Info) {
            Reservation_Info resIter = (Reservation_Info) iter;
            if (resIter.type == type && resIter.id == id) {
               return iter;
            }
         } else {
            assert (false);
         }
      }
      return null;
   }

   public boolean remove(CacheWrapper cache, E element) {
      Cons<E> oldElems = getElements(cache);
      Cons<E> newElems = oldElems.removeFirst(element);

      if (oldElems == newElems) {
         return false;
      } else {
         putElements(cache, newElems);
         return true;
      }
   }

   public Iterator<E> iterator(CacheWrapper cache) {
      List<E> snapshot = new ArrayList<E>();
      for (E element : getElements(cache))
         snapshot.add(element);
      Collections.reverse(snapshot);
      return snapshot.iterator();
   }
}
