package org.radargun.stages.stressors.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.TransactionFactory;
import org.radargun.stages.stressors.AbstractBenchmarkStressor;
import org.radargun.stages.stressors.Parameters;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by: Fabio Perfetti E-mail: perfabio87@gmail.com Date: 4/18/13
 */

public abstract class Producer<T extends AbstractBenchmarkStressor, S extends Parameters> implements IProducer {

   protected static Log log = LogFactory.getLog(Producer.class);
   protected final int id;
   private AtomicBoolean running = new AtomicBoolean(false);
   protected T stressor;
   protected S parameter;
   protected TransactionFactory factory;


   public Producer(int _id, T stressor, S parameter, TransactionFactory factory) {
      id = _id;
      this.stressor = stressor;
      this.parameter = parameter;
      this.factory = factory;
   }

   public void run() {
      if (!running.compareAndSet(false, true)) {
         log.warn("Someone is trying to execute a producer already started!! " +
                        "Thread name:" + Thread.currentThread().getName() + ". Skipping");
         return;
      } else {
         if (log.isDebugEnabled()) {
            log.info("Starting " + Thread.currentThread().getName() + " with rate of " + getSleepTime());
         }
      }

      while (running.get()) {
         int reqType = factory.nextTransaction();
         RequestType request = createRequestType(reqType);

         stressor.addToQueue(request);
         stressor.countJobs.incrementAndGet();
         sleep();
         // chiamando interrupt si imposta la variable running a false, facendo uscire il thread dal ciclo.
         // >>> PER EVITARE ULTERIORI GIRI DA PARTE DEL PRODUCER, TENERE LA CHIAMATA sleep() A FONDO CICLO <<<
      }
      log.info(Thread.currentThread().getName() + "(" + Thread.currentThread().getId() + ")" + " ended");
   }

   protected abstract double getSleepTime();

   protected abstract void sleep();

   protected abstract RequestType createRequestType(int reqType);

   public abstract void doNotify();

   public void interrupt() {
      if (running.compareAndSet(true, false)) {
         log.warn("Inserisco richiesta di tipo 9999 per terminare e setto running a false!");
         RequestType request = createRequestType(9999);
         stressor.addToQueue(request);
         // ora il producer è pronto per morire
      }
   }

   @Override
   public String toString() {
      return "Sono il Producer con id: " + id;
   }
}
