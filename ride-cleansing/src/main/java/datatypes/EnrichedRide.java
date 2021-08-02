package datatypes;

import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

import java.util.Objects;

public class EnrichedRide extends TaxiRide {
    private static final long serialVersionUID = -5350047780419384787L;
    public int startCell;
    public int endCell;

    public EnrichedRide() {
    }

    public EnrichedRide(TaxiRide ride) {
        this.rideId = ride.rideId;
        this.isStart = ride.isStart;
        this.startTime = ride.startTime;
        this.endTime = ride.endTime;
        this.startLon = ride.startLon;
        this.startLat = ride.startLat;
        this.endLon = ride.endLon;
        this.endLat = ride.endLat;
        this.passengerCnt = ride.passengerCnt;
        this.taxiId = ride.taxiId;
        this.driverId = ride.driverId;


        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
        this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
    }

    @Override
    public String toString() {
        return super.toString() +
               "," +
               startCell +
               "," +
               endCell;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        EnrichedRide that = (EnrichedRide) o;
        return startCell == that.startCell && endCell == that.endCell;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), startCell, endCell);
    }
}
