package org.radargun.portings.tpcc;

import org.radargun.stages.stressors.exceptions.ApplicationException;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public class ElementNotFoundException extends ApplicationException {

   public ElementNotFoundException() {
      super();
   }

   public ElementNotFoundException(String message) {
      super(message);
   }

   public ElementNotFoundException(String message, Throwable cause) {
      super(message, cause);
   }

   public ElementNotFoundException(Throwable cause) {
      super(cause);
   }

   @Override
   public boolean allowsRetry() {
      return false;
   }
}
