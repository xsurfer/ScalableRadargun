package org.radargun.stages.synthetic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.ITransaction;
import org.radargun.TransactionFactory;
import org.radargun.stages.stressors.KeyGenerator;
import org.radargun.stages.stressors.producer.RequestType;
import org.radargun.stages.stressors.syntethic.SyntheticParameters;
import org.radargun.utils.Utils;

import java.util.Random;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticXactFactory implements TransactionFactory {

    private static Log log = LogFactory.getLog(SyntheticXactFactory.class);

    protected Random rnd = new Random();
    protected KeyGenerator keyGenerator;
    protected int threadIndex;

    protected final SyntheticParameters parameters;

    public SyntheticXactFactory(SyntheticParameters params, int threadIndex) {
        this.parameters = params;
        this.keyGenerator = (KeyGenerator) Utils.instantiate( params.getKeyGeneratorClass() );
        this.threadIndex = threadIndex;
    }


    @Override
    public int nextTransaction() {
        if (!parameters.getCacheWrapper().isTheMaster() || (1 + rnd.nextInt(100)) > parameters.getWritePercentage())
            return xactClass.RO.getId();
        return xactClass.WR.getId();
    }


    @Override
    public ITransaction generateTransaction(RequestType type) {
        SyntheticXact toRet;
        XactOp[] ops;

        if (type.getTransactionType() == xactClass.RO.getId()) {
            ops = buildReadSet();
            toRet = new SyntheticXact(true);
        } else {
            ops = buildReadWriteSet();
            toRet = new SyntheticXact(false);
        }


        toRet.setOps(ops);
        toRet.setClazz(type.getTransactionType());

        if(log.isTraceEnabled()){
            log.trace("New xact built " + toRet.toString());
        }
        return toRet;
    }

    @Override
    public ITransaction choiceTransaction() {
        int txType = nextTransaction();
        RequestType requestType = new RequestType(System.nanoTime(), txType);

        return generateTransaction(requestType);
    }


    /**
     * This method builds a set containing keys to be read and <key, value> pairs to be written.  Put operations are uniformly distributed across read operations
     * (should work also with numReads<numWrites as long as there is at least one read).
     * If blind writes are not allowed, then writes are delayed till the first read operation. After that point, put operations are allowed and write always on the last read value
     * (even multiple times in a row, for simplicity)
     * @return a readWriteSet
     */
    protected XactOp[] buildReadWriteSet() {
        int toDoRead = parameters.getUpdateXactReads();
        int toDoWrite = parameters.getUpdateXactWrites();
        int toDo = toDoRead + toDoWrite;
        int total=toDo;
        int writePerc = (int) (100 *  (((double) toDoWrite) / ((double) (toDo))));

        int numKeys = parameters.getNumberOfAttributes();
        boolean allowBlindWrites = parameters.isAllowBlindWrites();
        XactOp[] ops = new XactOp[toDo];
        boolean doPut;
        int size = parameters.getSizeOfAnAttribute();
        boolean canWrite = false;
        int keyToAccess;
        int lastRead = 0;
        Object value;
        for (int i = 0; i < total; i++) {
            //Determine if you can read or not

            if (toDo == toDoWrite)      //I have only puts left
                doPut = true;
            else if (toDo == toDoRead)  //I have only reads left
                doPut = false;
            else if (allowBlindWrites) {     //I choose uniformly
                doPut = rnd.nextInt(100) < writePerc;
            } else {
                if (!canWrite) {   //first read
                    doPut = false;
                    canWrite = true;
                } else {
                    doPut = rnd.nextInt(100) < writePerc;
                }
            }
            if (doPut) {  //xact can write
                if (allowBlindWrites) { //xact can choose what it wants
                    keyToAccess = rnd.nextInt(numKeys);
                } else { //xact has to write something it has already read
                    keyToAccess = lastRead;
                }
            } else {    //xact reads
                keyToAccess = rnd.nextInt(numKeys);
                lastRead = keyToAccess;
            }
            value = doPut?   generateRandomString(size):"";
            ops[i] = new XactOp(keyToAccess, value , doPut);
            toDo--;
            if (doPut) {
                toDoWrite--;
            } else {
                toDoRead--;
            }
        }
        return ops;
    }

    private XactOp[] buildReadSet() {
        int numR = parameters.getReadOnlyXactSize();
        XactOp[] ops = new XactOp[numR];
        Object key;
        int keyToAccess;
        Random r = rnd;
        int numKeys = parameters.getNumberOfAttributes();
        int nodeIndex = parameters.getNodeIndex();

        for (int i = 0; i < numR; i++) {
            keyToAccess = r.nextInt(numKeys);
            key = keyGenerator.generateKey(nodeIndex, threadIndex, keyToAccess);
            ops[i] = new XactOp(key, "", false);
        }
        return ops;
    }

    protected String generateRandomString(int size) {
        // each char is 2 bytes
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size / 2; i++) sb.append((char) (64 + rnd.nextInt(26)));
        return sb.toString();
    }

}
