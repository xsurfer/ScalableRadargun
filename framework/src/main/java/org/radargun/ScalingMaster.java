package org.radargun;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.config.MasterConfig;
import org.radargun.stages.WebSessionBenchmarkStage;
import org.radargun.state.MasterState;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This is the master that will coordonate the {@link Slave}s in order to run the benchmark.
 * <p/>
 * Questo è il master, che coordina tutti gli slave. Funziona in questo modo:
 * prepara la socket dopodichè inizia la fase di discovery; una volta raggiunto il numero max di slave per il cluster
 * starta un thread che inizierà a far svolgere tutte attività come di consueto.
 * La fase di discovery non muore ma resta in discovery e quando trova 1 o più slaves che si vogliono aggiungere a runtime
 * starta un altro thread che prima di tutto va ad aggiornare la configurazione principale rappresentata da {@link MasterState}
 * dopodichè inizierà ad eseguiro lo stack di stages presenti nel scalingStagesRef che è una variabile locale al thread
 * <p/>
 * L'istanza di questa classe verrà usata in vari thread...ATTENTI ALLA SINCRONIZZAZIONE...per il resto speriamo tutto bene,
 * Buon Natale!
 *
 * @author Mircea.Markus@jboss.com
 */
public class ScalingMaster {

    private static ScalingMaster instance;

    private static Log log = LogFactory.getLog(ScalingMaster.class);
    public static final int DEFAULT_PORT = 2103;

    volatile MasterConfig masterConfig;

    private ServerSocketChannel serverSocketChannel;

    /**
     * contiene i canali verso tutti gli slave: ATTENTO ALLA SYNC TRA THREAD*
     */
    volatile List<SocketChannel> slaves = new ArrayList<SocketChannel>();

    volatile List<SocketChannel> slavesReadyToMerge = new ArrayList<SocketChannel>();

    volatile boolean stopped = false;

    volatile Map<SocketChannel, ByteBuffer> writeBufferMap = new HashMap<SocketChannel, ByteBuffer>();
    volatile Map<SocketChannel, ByteBuffer> readBufferMap = new HashMap<SocketChannel, ByteBuffer>();

    private List<DistStageAck> responses = new ArrayList<DistStageAck>();

    private Selector discoverySelector;
    volatile Selector communicationSelector;

    // questa variabile contiene informazioni (running) sulle istanze dei thread responsabili di far fare il join di slaves
    public ThreadLocal<Boolean> quickThreadRunning = new ThreadLocal<Boolean>(){
        protected Boolean initialValue() {
            return new Boolean(true);
        }
    };

    /**
     * Mappa utile per associare uno slave a un indice *
     */
    volatile Map<SocketChannel, Integer> slave2Index = new HashMap<SocketChannel, Integer>();

    public volatile MasterState state;

    private static final int DEFAULT_READ_BUFF_CAPACITY = 1024;

    private ScalingMaster(MasterConfig masterConfig) {
        this.masterConfig = masterConfig;
        state = new MasterState(masterConfig);
        Runtime.getRuntime().addShutdownHook(new ShutDownHook("Master process"));
    }

    public static ScalingMaster getMaster(MasterConfig masterConfig) {
        if (instance == null) {
            instance = new ScalingMaster(masterConfig);
        }
        return instance;
    }

    public void start() throws Exception {
        log.info("E' INIZIATO IL MASTER...eseguirà solo l'apertura della socket e la fase di discovery!!");
        try {
            startServerSocket();
            runDiscoveryFabio();
        } finally {
            releseResources();
        }
    }

    /**
     * RunDiscovery dovrà girare finchè il test non viene stoppato
     * Quando viene raggiunto il numero di slave sufficente per iniziare allora viene creato un nuovo thread con un suo stato
     * Quando uno nuovo slave si aggiunge allora viene creato un nuovo thread con nuovo stato!!
     */
    private void runDiscoveryFabio() throws IOException {
        int extraSlaves = 0;
        discoverySelector = Selector.open();
        serverSocketChannel.register(discoverySelector, SelectionKey.OP_ACCEPT);

        /** PRIMA FASE: resto in attesa del numero di slave sufficente per startare il cluster**/
        while (slaves.size() < masterConfig.getSlaveCount()) {
            log.info("Waiting for " + (masterConfig.getSlaveCount() - slaves.size()) + " slaves to register to the master.");
            discoverySelector.select();
            Set<SelectionKey> keySet = discoverySelector.selectedKeys();
            Iterator<SelectionKey> it = keySet.iterator();
            while (it.hasNext()) {                                       //Seleziono solamente le chiavi con OP_ACCEPT
                SelectionKey selectionKey = it.next();
                it.remove();
                if (!selectionKey.isValid()) {
                    continue;
                }
                ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
                SocketChannel socketChannel = srvSocketChannel.accept();
                slaves.add(socketChannel);      /* aggiungo sia alla variabile che contiene tutti gli slave*/
                slave2Index.put(socketChannel, (slaves.size() - 1));
                this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
                log.info("Discovery: IL readBufferMap ha size: " + this.readBufferMap.size());
                if (log.isTraceEnabled())
                    log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
            }
        }
        /** ok adesso siamo pronti per iniziare a formare il cluster **/
        log.info("Connection established from " + slaves.size() + " slaves.");
        log.info("Sono pronto per formare il cluster; gli slave sono: " + slaves.size());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Runnable worker = new ClusterExecutor(new ArrayList<SocketChannel>(slaves));
        executor.execute(worker);


        /** adesso resto in attesa di slaves ritardatari **/
        List<SocketChannel> tmpSlaves = new ArrayList<SocketChannel>(); // variabile che contiene i nuovi slaves da passare al nuovo thread
        while (!stopped) {
            log.info("Waiting for other slaves to register to the master.");
            discoverySelector.select();
            Set<SelectionKey> keySet = discoverySelector.selectedKeys();
            Iterator<SelectionKey> it = keySet.iterator();
            while (it.hasNext()) {                                       //Seleziono solamente le chiavi con OP_ACCEPT
                SelectionKey selectionKey = it.next();
                it.remove();
                if (!selectionKey.isValid()) {
                    continue;
                }
                log.info("*---*---* NEW SLAVE DISCOVERED *---*---*");
                ServerSocketChannel srvSocketChannel = (ServerSocketChannel) selectionKey.channel();
                SocketChannel socketChannel = srvSocketChannel.accept();
                slaves.add(socketChannel);      /* aggiungo sia alla variabile che contiene tutti gli slave*/
                tmpSlaves.add(socketChannel);   /* sia alla variabile temporanea che contiene un sott'insieme */
                slave2Index.put(socketChannel, (slaves.size() - 1));
                this.readBufferMap.put(socketChannel, ByteBuffer.allocate(DEFAULT_READ_BUFF_CAPACITY));
                log.info("Discovery: IL readBufferMap ha size: " + this.readBufferMap.size());
                if (log.isTraceEnabled())
                    log.trace("Added new slave connection from: " + socketChannel.socket().getInetAddress());
            }
            log.info("Connection established from " + tmpSlaves.size() + " new slaves.");
            executor = Executors.newSingleThreadExecutor();
            worker = new QuickExecutor(tmpSlaves);
            executor.execute(worker);

            // per ora lascio questo...potrebbe esserci un NullException
            tmpSlaves = new ArrayList<SocketChannel>();
        }
        return;
    }

    public void releaseResourcesAndExit() {
        releseResources();
        ShutDownHook.exit(0);
    }

    private void releseResources() {
        try {
            discoverySelector.close();
        } catch (Throwable e) {
            log.warn(e);
        }
//        try {
//            communicationSelector.close();
//        } catch (Throwable e) {
//            log.warn(e);
//        }
        for (SocketChannel sc : slaves) {
            try {
                sc.socket().close();
            } catch (Throwable e) {
                log.warn(e);
            }
        }

        try {
            if (serverSocketChannel != null) serverSocketChannel.socket().close();
        } catch (Throwable e) {
            log.warn(e);
        }
    }



    private void startServerSocket() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        InetSocketAddress address;
        if (masterConfig.getHost() == null) {
            address = new InetSocketAddress(masterConfig.getPort());
        } else {
            address = new InetSocketAddress(masterConfig.getHost(), masterConfig.getPort());
        }
        serverSocketChannel.socket().bind(address);
        log.info("Master started and listening for connection on: " + address);
        log.info("Waiting 5 seconds for server socket to open completely");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            // ignore
        }
    }
}
