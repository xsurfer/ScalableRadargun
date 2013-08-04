package org.radargun.stages.synthetic;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public enum xactClass {

    RO(0), WR(1);

    private int id;

    private xactClass(int id){
        this.id = id;
    }

    public int getId(){
        return id;
    }

}
