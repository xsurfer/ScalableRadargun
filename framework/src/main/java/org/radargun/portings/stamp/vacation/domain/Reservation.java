package org.radargun.portings.stamp.vacation.domain;

import org.radargun.CacheWrapper;
import org.radargun.portings.stamp.vacation.OpacityException;
import org.radargun.portings.stamp.vacation.Vacation;

import java.io.Serializable;
import java.util.UUID;

public class Reservation implements Comparable<Reservation>, Serializable {
    /* final */ int id;
    /* final */ String PREFIX; 
    static final String NUM_USED = "numUsed";
    static final String NUM_FREE = "numFree";
    static final String NUM_TOTAL = "numTotal";
    static final String PRICE = "price";

    public Reservation() { }
    
    public Reservation(CacheWrapper cache, String type, int id, int numTotal, int price) {
	this.id = id;
	this.PREFIX = UUID.randomUUID().toString() + ":" + "Reservation:" + type + ":" + id + ":";
	Vacation.put(cache, PREFIX + NUM_USED, 0);
	Vacation.put(cache, PREFIX + NUM_FREE, numTotal);
	Vacation.put(cache, PREFIX + NUM_TOTAL, numTotal);
	Vacation.put(cache, PREFIX + PRICE, price);
	checkReservation(cache);
    }
    
    public Integer getNumUsed(CacheWrapper cache) {
	return (Integer) Vacation.get(cache, PREFIX + NUM_USED);
    }
    
    public Integer getNumFree(CacheWrapper cache) {
	return (Integer) Vacation.get(cache, PREFIX + NUM_FREE);
    }
    
    public Integer getNumTotal(CacheWrapper cache) {
	return (Integer) Vacation.get(cache, PREFIX + NUM_TOTAL);
    }
    
    public Integer getPrice(CacheWrapper cache) {
	return (Integer) Vacation.get(cache, PREFIX + PRICE);
    }
    
    public void putNumUsed(CacheWrapper cache, Integer value) {
	Vacation.put(cache, PREFIX + NUM_USED, value);
    }
    
    public void putNumFree(CacheWrapper cache, Integer value) {
	Vacation.put(cache, PREFIX + NUM_FREE, value);
    }
    
    public void putNumTotal(CacheWrapper cache, Integer value) {
	Vacation.put(cache, PREFIX + NUM_TOTAL, value);
    }
    
    public void putPrice(CacheWrapper cache, Integer value) {
	Vacation.put(cache, PREFIX + PRICE, value);
    }

    public void checkReservation(CacheWrapper cache) {
	int numUsed = this.getNumUsed(cache);
	if (numUsed < 0) {
	    throw new OpacityException();
	}

	int numFree = this.getNumFree(cache);
	if (numFree < 0) {
	    throw new OpacityException();
	}

	int numTotal = this.getNumTotal(cache);
	if (numTotal < 0) {
	    throw new OpacityException();
	}

	if ((numUsed + numFree) != numTotal) {
	    throw new OpacityException();
	}

	int price = this.getPrice(cache);
	if (price < 0) {
	    throw new OpacityException();
	} 
    }

    boolean reservation_addToTotal(CacheWrapper cache, int num) {
	if (getNumFree(cache) + num < 0) {
	    return false;
	}

	putNumFree(cache, getNumFree(cache) + num);
	putNumTotal(cache, getNumTotal(cache) + num);
	checkReservation(cache);
	return true;
    }

    public boolean reservation_make(CacheWrapper cache) {
	if (getNumFree(cache) < 1) {
	    return false;
	}
	putNumUsed(cache, getNumUsed(cache) + 1);
	putNumFree(cache, getNumFree(cache) - 1);
	checkReservation(cache);
	return true;
    }

    boolean reservation_cancel(CacheWrapper cache) {
	if (getNumUsed(cache) < 1) {
	    return false;
	}
	putNumUsed(cache, getNumUsed(cache) - 1);
	putNumFree(cache, getNumFree(cache) + 1);
	checkReservation(cache);
	return true;
    }

    boolean reservation_updatePrice(CacheWrapper cache, int newPrice) {
	if (newPrice < 0) {
	    return false;
	}

	putPrice(cache, newPrice);
	checkReservation(cache);
	return true;
    }

    int reservation_compare(Reservation aPtr, Reservation bPtr) {
	return aPtr.id - bPtr.id;
    }

    int reservation_hash() {
	return id;
    }

    @Override
    public int compareTo(Reservation arg0) {
	int myId = this.id;
	int hisId = arg0.id;
	if (myId < hisId) {
	    return -1;
	} else if (myId == hisId) {
	    return 0;
	} else {
	    return 1;
	}
    }

}
