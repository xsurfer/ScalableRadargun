package org.radargun.workloadGenerator;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public class MuleSystem implements SystemType {

    private long thinktime = 0;

    @Override
    public String getType() {
        return SystemType.MULE;
    }

    public void setThinktime(long val){ thinktime = val; }
    public long getThinkTime(){ return thinktime; }

}
