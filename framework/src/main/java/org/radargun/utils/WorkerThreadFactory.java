package org.radargun.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ThreadFactory;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 3/22/13
 */
public class WorkerThreadFactory implements ThreadFactory {
    private int counter=0;
    private String prefix="";
    private final boolean unique;
    private static final Log log = LogFactory.getLog(WorkerThreadFactory.class);

    /**
     * This constructor creates a NON UNIQUE WorkerThreadFactory. Please for clarity use the explicit constructor
     * @param prefix
     */
    @Deprecated
    public WorkerThreadFactory(String prefix){
        this.prefix=prefix;
        this.unique = false;
    }

    /**
     * This constructor creates a WorkerThreadFactory.<br/>
     * It should create unique thread (thread's name) or multiple thread setting its name to 'prefix-<counter>'
     * @param prefix
     * @param unique
     */
    public WorkerThreadFactory(String prefix, boolean unique){
        this.prefix=prefix;
        this.unique = unique;
    }

    @Override
    public Thread newThread(Runnable r){
        if(unique){
            return new Thread(r,prefix);
        } else {
            return new Thread(r,prefix+"-"+counter++);
        }
    }
}