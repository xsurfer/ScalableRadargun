package org.radargun.stressors.microbenchmark;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Random;

import org.radargun.CacheWrapper;
import org.radargun.portings.microbenchmark.transaction.AddTransaction;
import org.radargun.portings.microbenchmark.transaction.ContainsTransaction;
import org.radargun.portings.microbenchmark.transaction.MicrobenchmarkTransaction;
import org.radargun.portings.microbenchmark.transaction.RemoveTransaction;
import org.radargun.stressors.AbstractCacheWrapperStressor;

public class MicrobenchmarkStressor extends AbstractCacheWrapperStressor implements Runnable {

    public static final int TEST_PHASE = 2;
    public static final int SHUTDOWN_PHASE = 3;

    private CacheWrapper cacheWrapper;
    private long restarts = 0;
    private long steps = 0;

    private int range;
    private int writeRatio;

    private boolean m_write = true;
    private int m_last;
    private Random m_random = new Random();

    volatile protected int m_phase = TEST_PHASE;

    public void setCacheWrapper(CacheWrapper cacheWrapper) {
        this.cacheWrapper = cacheWrapper;
    }

    @Override
    public void run() {
        stress(cacheWrapper);
    }

    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }

        this.cacheWrapper = wrapper;

        while (m_phase == TEST_PHASE) {
            step(TEST_PHASE);
            steps++;
        }

        Map<String, String> results = new LinkedHashMap<String, String>();

        return results;
    }

    protected void step(int phase) {
        int i = m_random.nextInt(100);
        if (i < writeRatio) {
            if (m_write) {
                m_last = m_random.nextInt(range);
                if (processTransaction(cacheWrapper, new AddTransaction(m_last)))
                    m_write = false;
            } else {
                processTransaction(cacheWrapper, new RemoveTransaction(m_last));
                m_write = true;
            }
        } else {
            processTransaction(cacheWrapper, new ContainsTransaction(m_last));
        }
    }

    private boolean processTransaction(CacheWrapper wrapper, MicrobenchmarkTransaction transaction) {
        boolean successful = true;
        boolean result = false;

        while (true) {
            if (m_phase == SHUTDOWN_PHASE) {
                return false;
            }
            result = false;
            cacheWrapper.startTransaction(transaction.isReadOnly());
            try {
                result = transaction.executeTransaction(cacheWrapper);
            } catch (Throwable e) {
        	e.printStackTrace();
                successful = false;
            }

            try {
                cacheWrapper.endTransaction(successful);

                if (!successful) {
                    setRestarts(getRestarts() + 1);
                }
            } catch (Throwable rb) {
                setRestarts(getRestarts() + 1);
                successful = false;
            }

            if (! successful) {
                successful = true;
            } else { 
                break;
            }
        }

        return result;
    }

    @Override
    public void destroy() throws Exception {
        cacheWrapper.empty();
        cacheWrapper = null;
    }

    public long getRestarts() {
        return restarts;
    }

    public void setRestarts(long restarts) {
        this.restarts = restarts;
    }

    public long getSteps() {
        return steps;
    }

    public void setSteps(long steps) {
        this.steps = steps;
    }

    public int getRange() {
        return range;
    }

    public void setRange(int range) {
        this.range = range;
    }
    
    public int getWriteRatio() {
        return writeRatio;
    }

    public void setWriteRatio(int writeRatio) {
        this.writeRatio = writeRatio;
    }

    public boolean isM_write() {
        return m_write;
    }

    public void setM_write(boolean m_write) {
        this.m_write = m_write;
    }

    public int getM_last() {
        return m_last;
    }

    public void setM_last(int m_last) {
        this.m_last = m_last;
    }

    public Random getM_random() {
        return m_random;
    }

    public void setM_random(Random m_random) {
        this.m_random = m_random;
    }

    public int getM_phase() {
        return m_phase;
    }

    public void setM_phase(int m_phase) {
        this.m_phase = m_phase;
    }

}
