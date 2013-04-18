package org.radargun.stressors.exceptions;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 4/17/13
 */
public abstract class ApplicationException extends Exception {

    public ApplicationException() {
        super();
    }

    public ApplicationException(String m){
        super(m);
    }

    public ApplicationException(String m, Throwable t){
        super(m,t);
    }

    public ApplicationException(Throwable cause) {
        super(cause);
    }

    public abstract boolean allowsRetry();

}
