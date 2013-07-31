package org.radargun.cachewrappers;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;

import java.util.Observable;

/**
 * Created by: Fabio Perfetti
 * E-mail: perfabio87@gmail.com
 * Date: 7/3/13
 */

@Listener
public class InfinispanListener extends Observable {
    @ViewChanged
    public void viewChanged(ViewChangedEvent e) {
        // add something here if you want execute something just after a new node is joined

        // reconfiguration is needed
        //System.out.println("View Changed!" );
        //setChanged();
        //notifyObservers( new Integer(CacheWrapper.VIEW_CHANGED) );
    }
}