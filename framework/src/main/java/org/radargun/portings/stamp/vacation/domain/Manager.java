package org.radargun.portings.stamp.vacation.domain;

import org.radargun.CacheWrapper;
import org.radargun.portings.stamp.vacation.Definitions;
import org.radargun.portings.stamp.vacation.OpacityException;
import org.radargun.portings.stamp.vacation.Vacation;

import java.io.Serializable;
import java.util.Iterator;

public class Manager implements Serializable {
    public static final String CARS = "carTable";
    public static final String ROOMS = "roomsTable";
    public static final String FLIGHTS = "flightsTable";
    public static final String CUSTOMERS = "customersTable";

    public Manager() { }
    
    void putCustomer(CacheWrapper cache, int id, Customer val) {
	Vacation.put(cache, CUSTOMERS + ":" + id, val);
    }
    
    Customer getCustomer(CacheWrapper cache, int id) {
	return (Customer) Vacation.get(cache, CUSTOMERS + ":" + id);
    }
    
    void putReservation(CacheWrapper cache, String table, int id, Reservation val) {
	Vacation.put(cache, table + ":" + id, val);
    }
    
    Reservation getReservation(CacheWrapper cache, String table, int id) {
	return (Reservation) Vacation.get(cache, table + ":" + id);
    }
    
    boolean addReservation(CacheWrapper cache, String table, String type, int id, int num, int price) {
	Reservation reservation = getReservation(cache, table, id);

	if (reservation == null) {
	    /* Create new reservation */
	    if (num < 1 || price < 0) {
		return false;
	    }
	    reservation = new Reservation(cache, type, id, num, price);
	    putReservation(cache, table, id, reservation);
	} else {
	    /* Update existing reservation */
	    if (!reservation.reservation_addToTotal(cache, num)) {
		return false;
	    }
	    if (reservation.getNumTotal(cache) == 0) {
	    } else {
		reservation.reservation_updatePrice(cache, price);
	    }
	}

	return true;
    }

    public boolean manager_addCar(CacheWrapper cache, int carId, int numCars, int price) {
	return addReservation(cache, CARS, "car", carId, numCars, price);
    }

    public boolean manager_deleteCar(CacheWrapper cache, int carId, int numCar) {
	return addReservation(cache, CARS, "car", carId, -numCar, -1);
    }

    public boolean manager_addRoom(CacheWrapper cache, int roomId, int numRoom, int price) {
	return addReservation(cache, ROOMS, "room", roomId, numRoom, price);
    }

    public boolean manager_deleteRoom(CacheWrapper cache, int roomId, int numRoom) {
	return addReservation(cache, ROOMS, "room", roomId, -numRoom, -1);
    }

    public boolean manager_addFlight(CacheWrapper cache, int flightId, int numSeat, int price) {
	return addReservation(cache, FLIGHTS, "flight", flightId, numSeat, price);
    }

    public boolean manager_deleteFlight(CacheWrapper cache, int flightId) {
	Reservation reservation = getReservation(cache, FLIGHTS, flightId);
	if (reservation == null) {
	    return false;
	}

	if (reservation.getNumUsed(cache) > 0) {
	    return false; /* somebody has a reservation */
	}

	return addReservation(cache, FLIGHTS, "flight", flightId, -reservation.getNumTotal(cache), -1);
    }

    public boolean manager_addCustomer(CacheWrapper cache, int customerId) {
	Customer customer = getCustomer(cache, customerId);

	if (customer != null) {
	    return false;
	}

	customer = new Customer(cache, customerId);
	putCustomer(cache, customerId, customer);

	return true;
    }

    private String translateToTree(int type) {
	if (type == Definitions.RESERVATION_CAR) {
	    return CARS;
	} else if (type == Definitions.RESERVATION_FLIGHT) {
	    return FLIGHTS;
	} else if (type == Definitions.RESERVATION_ROOM) {
	    return ROOMS;
	}
	throw new RuntimeException("Did not find matching type for: " + type);
    }
    
    public boolean manager_deleteCustomer(CacheWrapper cache, int customerId) {
	Customer customer = getCustomer(cache, customerId);
	List_t<Reservation_Info> reservationInfoList;
	boolean status;

	if (customer == null) {
	    return false;
	}

	/* Cancel this customer's reservations */
	reservationInfoList = customer.reservationInfoList;

	Iterator<Reservation_Info> iter = reservationInfoList.iterator(cache);
	while (iter.hasNext()) {
	    Reservation_Info reservationInfo = iter.next();
	    Reservation reservation = getReservation(cache, translateToTree(reservationInfo.type), reservationInfo.id);
	    if (reservation == null) {
		throw new OpacityException();
	    }
	    status = reservation.reservation_cancel(cache);
	    if (!status) {
		throw new OpacityException();
	    }
	}

	return true;
    }

    int queryNumFree(CacheWrapper cache, String table, int id) {
	int numFree = -1;
	Reservation reservation = getReservation(cache, table, id);
	if (reservation != null) {
	    numFree = reservation.getNumFree(cache);
	}

	return numFree;
    }

    int queryPrice(CacheWrapper cache, String table, int id) {
	int price = -1;
	Reservation reservation = getReservation(cache, table, id);
	if (reservation != null) {
	    price = reservation.getPrice(cache);
	}

	return price;
    }

    public int manager_queryCar(CacheWrapper cache, int carId) {
	return queryNumFree(cache, CARS, carId);
    }

    public int manager_queryCarPrice(CacheWrapper cache, int carId) {
	return queryPrice(cache, CARS, carId);
    }

    public int manager_queryRoom(CacheWrapper cache, int roomId) {
	return queryNumFree(cache, ROOMS, roomId);
    }

    public int manager_queryRoomPrice(CacheWrapper cache, int roomId) {
	return queryPrice(cache, ROOMS, roomId);
    }

    public int manager_queryFlight(CacheWrapper cache, int flightId) {
	return queryNumFree(cache, FLIGHTS, flightId);
    }

    public int manager_queryFlightPrice(CacheWrapper cache, int flightId) {
	return queryPrice(cache, FLIGHTS, flightId);
    }

    public int manager_queryCustomerBill(CacheWrapper cache, int customerId) {
	int bill = -1;
	Customer customer = getCustomer(cache, customerId);

	if (customer != null) {
	    bill = customer.customer_getBill(cache);
	}

	return bill;
    }

    boolean reserve(CacheWrapper cache, String table, int customerId, int id, int type) {
	Customer customer = getCustomer(cache, customerId);
	Reservation reservation = getReservation(cache, table, id);

	if (customer == null) {
	    return false;
	}

	if (reservation == null) {
	    return false;
	}

	if (!reservation.reservation_make(cache)) {
	    return false;
	}

	if (!customer.customer_addReservationInfo(cache, type, id, reservation.getPrice(cache))) {
	    /* Undo previous successful reservation */
	    boolean status = reservation.reservation_cancel(cache);
	    if (!status) {
		throw new OpacityException();
	    }
	    return false;
	}

	return true;
    }

    public boolean manager_reserveCar(CacheWrapper cache, int customerId, int carId) {
	return reserve(cache, CARS, customerId, carId, Definitions.RESERVATION_CAR);
    }

    public boolean manager_reserveRoom(CacheWrapper cache, int customerId, int roomId) {
	return reserve(cache, ROOMS, customerId, roomId, Definitions.RESERVATION_ROOM);
    }

    public boolean manager_reserveFlight(CacheWrapper cache, int customerId, int flightId) {
	return reserve(cache, FLIGHTS, customerId, flightId, Definitions.RESERVATION_FLIGHT);
    }

    boolean cancel(CacheWrapper cache, String table, int customerId, int id, int type) {
	Customer customer = getCustomer(cache, customerId);
	Reservation reservation = getReservation(cache, table, id);

	if (customer == null) {
	    return false;
	}

	if (reservation == null) {
	    return false;
	}

	if (!reservation.reservation_cancel(cache)) {
	    return false;
	}

	if (!customer.customer_removeReservationInfo(cache, type, id)) {
	    /* Undo previous successful cancellation */
	    boolean status = reservation.reservation_make(cache);
	    if (!status) {
		throw new OpacityException();
	    }
	    return false;
	}

	return true;
    }

    boolean manager_cancelCar(CacheWrapper cache, int customerId, int carId) {
	return cancel(cache, CARS, customerId, carId, Definitions.RESERVATION_CAR);
    }

    boolean manager_cancelRoom(CacheWrapper cache, int customerId, int roomId) {
	return cancel(cache, ROOMS, customerId, roomId, Definitions.RESERVATION_ROOM);
    }

    boolean manager_cancelFlight(CacheWrapper cache, int customerId, int flightId) {
	return cancel(cache, FLIGHTS, customerId, flightId, Definitions.RESERVATION_FLIGHT);
    }
    
}
