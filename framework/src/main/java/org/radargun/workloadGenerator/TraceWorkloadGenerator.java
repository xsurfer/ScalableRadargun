package org.radargun.workloadGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Generate a workload based on function f(t)=amplitude*sin(t)
 * User: Fabio Perfetti
 * Date: 3/18/13
 */
public class TraceWorkloadGenerator extends AbstractWorkloadGenerator {

    private static Log log = LogFactory.getLog(TraceWorkloadGenerator.class);

    private List<Integer> arrivalRates;

    private String file = null;
    public String getFile(){ return this.file; }

    public void setFile(String file){
        this.file = file;
        log.trace("Loading trace");
        arrivalRates = new ArrayList<Integer>();
        Scanner sc = null;
        try {
            sc = new Scanner(new File(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        while (sc.hasNextLong()) {
            int anInt = sc.nextInt();
            arrivalRates.add(anInt);
            log.trace("Found " + anInt);
        }
        log.info("Trace loaded");

    }

    @Override
    public int getCurrentArrivalRate() {
        int t = ((int) Math.floor(getTime())) % arrivalRates.size();
        int eval = (arrivalRates.get(t).intValue());
        log.trace("Time: " + getTime() + "s, ArrivalRate: " + eval);
        return eval;
    }

    @Override
    public TraceWorkloadGenerator clone(){
        return (TraceWorkloadGenerator) super.clone();
    }

}