package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.stages.AbstractBenchmarkStage;
import org.radargun.workloadGenerator.AbstractWorkloadGenerator;

import java.util.Map;
import java.util.Observable;
import java.util.Observer;

/**
 * @author Mircea Markus <mircea.markus@gmail.com>
 */
public abstract class AbstractCacheWrapperStressor implements CacheWrapperStressor {

    /* ****************** */
    /* *** ATTRIBUTES *** */
    /* ****************** */

    private static Log log = LogFactory.getLog(AbstractCacheWrapperStressor.class);

    private boolean sysMonitorEnabled = false;

    protected CacheWrapper cacheWrapper;


    /* ******************* */
    /* *** CONSTRUCTOR *** */
    /* ******************* */


    /* ****************** */
    /* ***** METHODS **** */
    /* ****************** */


    /* ******************* */
    /* *** TO OVERRIDE *** */
    /* ******************* */


    /* ******************** */
    /* *** GETTER/SETTER ** */
    /* ******************** */

    @Override
    public void setSysMonitorEnabled(boolean enabled) { sysMonitorEnabled = enabled; }

    @Override
    public boolean isSysMonitorEnabled() { return sysMonitorEnabled; }

}
