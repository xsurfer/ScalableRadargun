package org.radargun.stages;

import org.radargun.DistStageAck;

import java.net.InetAddress;
import java.util.Map;

/**
 * Default implementation for a distributed stage.
 *
 * @author Mircea.Markus@jboss.com
 */
public class DefaultDistStageAck implements DistStageAck {

   private int slaveIndex;
   private InetAddress slaveAddress;

   private boolean isError;
   private String errorMessage;
   private Object payload;

   private long initialTs;
   private long duration;

   private String remoteExceptionString;

   private String stageName;

   public DefaultDistStageAck(int slaveIndex, InetAddress slaveAddress, String stageName) {
      this.slaveIndex = slaveIndex;
      this.slaveAddress = slaveAddress;
      this.stageName = stageName;
   }

   /* Aggiunto da fabio in modo che quando il master aspetta degli ack, controlla se gli ack ricevuti fanno parte
   * dello stage corrente
   */
   @Override
   public String getStageName() {
      return stageName;
   }

   @Override
   public boolean isStoppedByJMX() {
      if (payload instanceof Map) {
         Map<String, Object> results = (Map<String, Object>) payload;
         Object stoppedByJmxObj = results.get("STOPPED");
         if (stoppedByJmxObj != null) {
            boolean stoppedByJmx = Boolean.parseBoolean(stoppedByJmxObj.toString());
            if (stoppedByJmx) {
               return true;
            }
         }
      }
      return false;
   }

    /* fine */

   public int getSlaveIndex() {
      return slaveIndex;
   }

   public boolean isError() {
      return isError;
   }

   public void setError(boolean error) {
      isError = error;
   }

   public String getRemoteExceptionString() {
      return remoteExceptionString;
   }

   public void setRemoteException(Throwable remoteException) {
      StackTraceElement[] stackTraceElements = remoteException.getStackTrace();
      if (stackTraceElements != null && stackTraceElements.length > 0) {
         remoteExceptionString = "\n";
         for (StackTraceElement ste : stackTraceElements) {
            remoteExceptionString += ste.toString() + "\n";
         }
      }
   }

   public String getErrorMessage() {
      return errorMessage;
   }

   public void setErrorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
   }

   public Object getPayload() {
      return payload;
   }

   public void setPayload(Object payload) {
      this.payload = payload;
   }

   @Override
   public String toString() {
      return "DefaultDistStageAck{" +
            "slaveIndex=" + slaveIndex +
            ", slaveAddress=" + slaveAddress +
            ", isError=" + isError +
            ", errorMessage='" + errorMessage + '\'' +
            ", payload=" + payload +
            ", remoteExceptionString=" + remoteExceptionString +
            '}';
   }

   public String getSlaveDescription() {
      return slaveAddress + "(" + slaveIndex + ")";
   }

   public void setDuration(long duration) {
      this.duration = duration;
   }

   public long getDuration() {
      return duration;
   }

   public void setInitialTs(long ts) {
      this.initialTs = ts;
   }

   public long getInitialTs() {
      return initialTs;
   }

}
