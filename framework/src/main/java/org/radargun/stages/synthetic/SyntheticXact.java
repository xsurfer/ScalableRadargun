package org.radargun.stages.synthetic;

import org.radargun.CacheWrapper;
import org.radargun.ITransaction;

import java.util.Arrays;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticXact implements ITransaction {

    private long initResponseTime;
    private long initServiceTime;
    public int clazz;
    private boolean isCommit;  //we track only commit-abort without considering also xact that can abort because of application logic (and might be not restarted, then)
    XactOp[] ops;

    private boolean isReadOnly;


    public SyntheticXact(boolean isreadOnly) {
        initResponseTime = System.nanoTime();
        initServiceTime = initResponseTime;
        this.isReadOnly = isreadOnly;
    }

    public XactOp[] getOps() {
        return ops;
    }

    public void setOps(XactOp[] ops) {
        this.ops = ops;
    }

    public long getInitResponseTime() {
        return initResponseTime;
    }

    public void setInitResponseTime(long initResponseTime) {
        this.initResponseTime = initResponseTime;
    }

    public long getInitServiceTime() {
        return initServiceTime;
    }

    public void setInitServiceTime(long initServiceTime) {
        this.initServiceTime = initServiceTime;
    }

    public int getClazz() {
        return clazz;
    }

    public void setClazz(int clazzId) {
        this.clazz = clazzId;
    }

    public boolean isCommit() {
        return isCommit;
    }

    public void setCommit(boolean commit) {
        isCommit = commit;
    }

//   public void executeLocally() throws Exception {
//      for(XactOp op:ops){
//         if(op.isPut())
//            cache.put(null,op.getKey(),op.getValue());
//         else
//            cache.get(null,op.getKey());
//      }
//   }

    @Override
    public String toString() {
        return "SyntheticXact{" +
                "initResponseTime=" + initResponseTime +
                ", initServiceTime=" + initServiceTime +
                ", clazz=" + clazz +
                ", isCommit=" + isCommit +
                ", ops=" + Arrays.toString(ops) +
                '}';
    }

    @Override
    public void executeTransaction(CacheWrapper cache) throws Throwable {
        for(XactOp op:ops){
            if(op.isPut())
                cache.put(null,op.getKey(),op.getValue());
            else
                cache.get(null,op.getKey());
        }

    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public int getType() {
        return 0;
    }
}