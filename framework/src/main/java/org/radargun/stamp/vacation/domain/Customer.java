package org.radargun.stamp.vacation.domain;

import java.io.Serializable;
import java.util.Iterator;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.OpacityException;


public class Customer implements Serializable {

    /* final */ int id;
    /* final */ List_t<Reservation_Info> reservationInfoList;

    public Customer() { }
    
    public Customer(CacheWrapper cache, int id) {
	this.id = id;
	reservationInfoList = new List_t<Reservation_Info>(cache, "List:" + this.id + ":elements");
    }

    int customer_compare(Customer aPtr, Customer bPtr) {
	return (aPtr.id - bPtr.id);
    }

    boolean customer_addReservationInfo(CacheWrapper cache, int type, int id, int price) {
	Reservation_Info reservationInfo = new Reservation_Info(type, id, price);

	reservationInfoList.add(cache, reservationInfo);
	return true;
    }

    boolean customer_removeReservationInfo(CacheWrapper cache, int type, int id) {
	Reservation_Info reservationInfo = reservationInfoList.find(cache, type, id);

	if (reservationInfo == null) {
	    return false;
	}

	boolean status = reservationInfoList.remove(cache, reservationInfo);
	if (!status) {
	    throw new OpacityException();
	}
	return true;
    }

    int customer_getBill(CacheWrapper cache) {
	int bill = 0;

	Iterator<Reservation_Info> iter = reservationInfoList.iterator(cache);
	while (iter.hasNext()) {
	    Reservation_Info it = iter.next();
	    bill += it.price;
	}

	return bill;
    }
}
