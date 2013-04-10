package org.radargun.stressors.commons;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/2/13
 */
public class StressorStats {

    protected long reads = 0L;
    public long getReads() {
        return reads;
    }
    public void incReads(){ reads++; }

    protected long writes = 0L;
    public long getWrites() {
        return writes;
    }
    public void incWrites(){ writes++; }

    protected int nrFailures = 0;
    public int getNrFailures() {
        return nrFailures;
    }
    public void incNrFailures(){ nrFailures++; }

    protected int nrWrFailures = 0;
    public int getNrWrFailures() {
        return nrWrFailures;
    }
    public void incNrWrFailures(){ nrWrFailures++; }

    protected int nrRdFailures = 0;
    public int getNrRdFailures() {
        return nrRdFailures;
    }
    public void incNrRdFailures(){ nrRdFailures++; }



    protected int nrWrFailuresOnCommit = 0;
    public int getNrWrFailuresOnCommit() {
        return nrWrFailuresOnCommit;
    }
    public void incNrWrFailuresOnCommit(){ nrWrFailuresOnCommit++; }

    protected int appFailures = 0;
    public int getAppFailures() {
        return appFailures;
    }
    public void incAppFailures(){ appFailures++; }



    protected long readDuration = 0L;
    public long getReadDuration() {
        return readDuration;
    }
    public void incReadDuration(long amount){ readDuration+=amount; }

    protected long writeDuration = 0L;
    public long getWriteDuration() {
        return writeDuration;
    }
    public void incWriteDuration(long amount){ writeDuration+=amount; }


    protected long successfulCommitWriteDuration = 0L;
    public long getSuccessfulCommitWriteDuration() {
        return successfulCommitWriteDuration;
    }
    public void incSuccessfulCommitWriteDuration(long amount){ successfulCommitWriteDuration += amount; }

    protected long abortedCommitWriteDuration = 0L;
    public long getAbortedCommitWriteDuration() {
        return abortedCommitWriteDuration;
    }
    public void incAbortedCommitWriteDuration(long amount){ abortedCommitWriteDuration += amount; }

    protected long commitWriteDuration = 0L;
    public long getCommitWriteDuration() {
        return commitWriteDuration;
    }
    public void incCommitWriteDuration(long amount){ commitWriteDuration += amount; }



    protected long writeServiceTime = 0L;
    public long getWriteServiceTime() {
        return writeServiceTime;
    }
    public void incWriteServiceTime(long amount){ writeServiceTime+=amount; }

    protected long readServiceTime = 0L;
    public long getReadServiceTime() {
        return readServiceTime;
    }
    public void incReadServiceTime(long amount){ readServiceTime+= amount; }



    protected long successfulWriteDuration = 0L;
    public long getSuccessfulWriteDuration() {
        return successfulWriteDuration;
    }
    public void incSuccessfulWriteDuration(long amount){ successfulWriteDuration+=amount; }

    protected long successfulReadDuration = 0L;
    public long getSuccessfulReadDuration() {
        return successfulReadDuration;
    }
    public void incSuccessfulReadDuration(long amount){ successfulReadDuration+=amount; }



    /* ******************* */
    /* *** QUEUE STATS *** */
    /* ******************* */

    protected long numWriteDequeued = 0L;
    public long getNumWriteDequeued() {
        return numWriteDequeued;
    }
    public void incNumWriteDequeued(){ numWriteDequeued++; }

    protected long numReadDequeued = 0L;
    public long getNumReadDequeued() {
        return numReadDequeued;
    }
    public void incNumReadDequeued(){ numReadDequeued++; }

    /* tempo di attesa in coda per le write */
    protected long writeInQueueTime = 0L;
    public long getWriteInQueueTime() {
        return writeInQueueTime;
    }
    public void incWriteInQueueTime(long amount){ writeInQueueTime+=amount; }

    /* tempo di attesa in coda per le read */
    protected long readInQueueTime = 0L;
    public long getReadInQueueTime() {
        return readInQueueTime;
    }
    public void incReadInQueueTime(long amount){ readInQueueTime+= amount; }




    protected long localTimeout = 0L;
    public long getLocalTimeout() {
        return localTimeout;
    }
    public void incLocalTimeout(){ localTimeout++; }

    protected long remoteTimeout = 0L;
    public long getRemoteTimeout() {
        return remoteTimeout;
    }
    public void incRemoteTimeout(){ remoteTimeout++; }

    protected long numBackOffs = 0L;
    public long getNumBackOffs() {
        return numBackOffs;
    }
    public void incNumBackOffs(){ numBackOffs++; }

    protected long backedOffTime = 0L;
    public long getBackedOffTime() {
        return backedOffTime;
    }
    public void incBackedOffTime(long amount){ backedOffTime+= amount; }

}
