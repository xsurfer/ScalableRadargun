package org.radargun.portings.stamp.vacation;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class Cons<E> implements Iterable<E>, Serializable {

   public final static <T> Cons<T> empty() {
      return new Cons<T>(true);
   }

   protected /* final */ boolean empty;
   protected /* final */ E first;
   protected /* final */ Cons<E> rest;

   public Cons() {

   }

   private Cons(boolean empty) {
      this.empty = true;
   }

   private Cons(E first, Cons<E> rest) {
      this.first = first;
      this.rest = rest;
   }

   public final Cons<E> cons(E elem) {
      return new Cons<E>(elem, this);
   }

   public final E first() {
      if (isEmpty()) {
         throw new EmptyListException();
      } else {
         return first;
      }
   }

   public final Cons<E> rest() {
      if (isEmpty()) {
         throw new EmptyListException();
      } else {
         return rest;
      }
   }

   public final Cons<E> removeFirst(Object elem) {
      Cons<E> found = member(elem);
      if (found == null) {
         return this;
      } else {
         return removeExistingCons(found);
      }
   }

   private final Cons<E> removeExistingCons(Cons<?> cons) {
      if (cons == this) {
         return rest;
      } else {
         // We have to allocate new Cons cells until we reach the cons to remove
         Cons<E> newCons = (new Cons<E>()).cons(first);
         Cons<E> next = rest;
         while (next != cons) {
            newCons = newCons.cons(next.first);
            next = next.rest;
         }

         // share the rest
         newCons = newCons.reverseInto(next.rest);
         return newCons;
      }
   }

   public final boolean isEmpty() {
      return empty;
   }

   public final Cons<E> member(Object elem) {
      Cons<E> iter = this;
      if (elem == null) {
         while (!iter.empty) {
            if (iter.first == null) {
               return iter;
            }
            iter = iter.rest;
         }
      } else {
         while (!iter.empty) {
            if (elem.equals(iter.first)) {
               return iter;
            }
            iter = iter.rest;
         }
      }
      return null;
   }

   public final Cons<E> reverseInto(Cons<E> tail) {
      Cons<E> result = tail;
      Cons<E> iter = this;
      while (!iter.empty) {
         result = result.cons(iter.first);
         iter = iter.rest;
      }
      return result;
   }

   public final Iterator<E> iterator() {
      return new ConsIterator<E>(this);
   }

   final static class ConsIterator<T> implements Iterator<T> {
      private Cons<T> current;

      ConsIterator(Cons<T> start) {
         this.current = start;
      }

      public final boolean hasNext() {
         return (!current.empty);
      }

      public final T next() {
         if (current.empty) {
            throw new NoSuchElementException();
         } else {
            T result = current.first;
            current = current.rest;
            return result;
         }
      }

      public final void remove() {
         throw new UnsupportedOperationException();
      }
   }
}
