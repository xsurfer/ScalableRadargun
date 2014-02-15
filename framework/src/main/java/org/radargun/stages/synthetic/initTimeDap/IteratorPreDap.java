package org.radargun.stages.synthetic.initTimeDap;


import org.radargun.stages.synthetic.XactOp;

import java.util.Iterator;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class IteratorPreDap implements Iterator<XactOp> {

   private XactOp[] ops;
   final int toDo;
   int current;

   public IteratorPreDap(XactOp[] ops) {
      this.ops = ops;
      toDo = ops.length;
      current = 0;
   }

   @Override
   public boolean hasNext() {
      return current < toDo;
   }

   @Override
   public XactOp next() {
      XactOp next = ops[current];
      current++;
      return next;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
   }
}
