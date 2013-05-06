package org.radargun.workloadGenerator;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/19/13
 */
public interface SystemType {

    public final static String OPEN = "open";
    public final static String CLOSED = "closed";
    public final static String MULE = "mule";

    public String getType();
}
