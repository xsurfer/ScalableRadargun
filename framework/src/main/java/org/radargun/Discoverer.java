package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.utils.WorkerThreadFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: Fabio Perfetti (perfabio87 [at] gmail.com) Date: 8/1/13 Time: 2:49 PM
 */
public class Discoverer {

   private static Log log = LogFactory.getLog(ClusterExecutor.class);

   private AtomicInteger slaveCounter = new AtomicInteger(0);

   private NewElasticMaster elasticMaster;

   private final ServerSocketChannel serverSocketChannel;
   private Selector discoverySelector;

   private AtomicBoolean running = new AtomicBoolean(false);


   public Discoverer(NewElasticMaster elasticMaster, ServerSocketChannel serverSocketChannel) {
      this.elasticMaster = elasticMaster;
      this.serverSocketChannel = serverSocketChannel;

      try {
         discoverySelector = Selector.open();
         serverSocketChannel.register(discoverySelector, SelectionKey.OP_ACCEPT);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   public void start() {
      if (running.compareAndSet(false, true)) {

         ExecutorService executors = Executors.newSingleThreadExecutor(new WorkerThreadFactory("Discoverer", true));
         executors.submit(new Runnable() {
            @Override
            public void run() {
               discovery();
            }
         });
         executors.shutdown();
         log.info("Discovery started!");

      } else {
         log.warn("Discovery already started!");
      }
   }

   public void stop(boolean releaseResources) {

      if (running.compareAndSet(true, false)) {
         log.info("Discovery stopped!");
         if (releaseResources) {
            releaseResources();
         }
      } else {
         log.warn("Discovery already stopped!");
      }
   }

   private void releaseResources() {
      try {
         discoverySelector.close();
      } catch (IOException e) {
         log.fatal(e, e);
      }
   }


   private void discovery() {

      while (running.get()) {

         log.info("Waiting for slaves to register to the master.");
         try {
            discoverySelector.select();
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
         Set<SelectionKey> keySet = discoverySelector.selectedKeys();
         dispatch(keySet);
      }

   }

   private void dispatch(Set<SelectionKey> keySet) {
      Iterator<SelectionKey> it = keySet.iterator();
      while (it.hasNext()) {
         SelectionKey selectionKey = it.next();
         it.remove();
         if (!selectionKey.isValid()) {
            continue;
         }
         ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
         SocketChannel socketChannel = null;
         try {
            socketChannel = srvSocketChannel.accept();
         } catch (IOException e) {
            log.fatal(e, e);
         }

         SlaveSocketChannel slaveSocketChannel = new SlaveSocketChannel(slaveCounter.getAndIncrement(), socketChannel);

         try {
            elasticMaster.slaveJoined(slaveSocketChannel);
         } catch (Exception e) {
            log.fatal(e, e);
         }


                /*
                ElasticMaster.this.slaves.add(socketChannel);
                slave2Index.put(socketChannel, (ElasticMaster.this.slaves.size() - 1));
                ElasticMaster.this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));

                log.info("Discovery: IL readBufferMap ha size: " + readBufferMap.size());
                */

         log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
      }

   }


//
//    public void discovery() throws IOException {
//        discoverySelector = Selector.open();
//        serverSocketChannel.register(discoverySelector, SelectionKey.OP_ACCEPT);
//        while (slaves.size() < masterConfig.getSlaveCount()) {
//            log.info("Awaiting registration from " + (masterConfig.getSlaveCount() - slaves.size()) + " slaves.");
//            discoverySelector.select();
//            Set<SelectionKey> keySet = discoverySelector.selectedKeys();
//            Iterator<SelectionKey> it = keySet.iterator();
//            while (it.hasNext()) {
//                SelectionKey selectionKey = it.next();
//                it.remove();
//                if (!selectionKey.isValid()) {
//                    continue;
//                }
//                ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
//                SocketChannel socketChannel = srvSocketChannel.accept();
//                slaves.add(socketChannel);
//                slave2Index.put(socketChannel, (slaves.size() - 1));
//                this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
//                if (log.isTraceEnabled())
//                    log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
//            }
//        }
//        log.info("Connection established from " + slaves.size() + " slaves.");
//    }


//    private void clusterDiscovery() throws IOException {
//        /** PRIMA FASE: resto in attesa del numero di slave sufficente per startare il cluster**/
//        while (ElasticMaster.this.slaves.size() < masterConfig.getSlaveCount()) {
//            log.info("Waiting for " + (masterConfig.getSlaveCount() - ElasticMaster.this.slaves.size()) + " slaves to register to the master.");
//            discoverySelector.select();
//            Set<SelectionKey> keySet = discoverySelector.selectedKeys();
//            Iterator<SelectionKey> it = keySet.iterator();
//            while (it.hasNext()) {                                       //Seleziono solamente le chiavi con OP_ACCEPT
//                SelectionKey selectionKey = it.next();
//                it.remove();
//                if (!selectionKey.isValid()) {
//                    continue;
//                }
//                ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
//                SocketChannel socketChannel = srvSocketChannel.accept();
//                ElasticMaster.this.slaves.add(socketChannel);
//                slave2Index.put(socketChannel, (ElasticMaster.this.slaves.size() - 1));
//                ElasticMaster.this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
//                log.info("Discovery: IL readBufferMap ha size: " + readBufferMap.size());
//                if (log.isTraceEnabled())
//                    log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
//            }
//        }
//        /** ok adesso siamo pronti per iniziare a formare il cluster **/
//        log.info("Connection established from " + ElasticMaster.this.slaves.size() + " slaves.");
//        log.info("Ready to start benchmarking on the cluster; Slave number: " + ElasticMaster.this.slaves.size());
//
//        ExecutorService executor = Executors.newSingleThreadExecutor(new WorkerThreadFactory("ClusterExecutor", true));
//        ElasticMaster.this.clusterExecutorThread = new ClusterExecutor(ElasticMaster.this.slaves);
//        executor.execute(ElasticMaster.this.clusterExecutorThread);
//        executor.shutdown();
//    }

}
