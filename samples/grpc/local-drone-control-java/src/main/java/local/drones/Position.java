package local.drones;

import java.util.Objects;

public final class Position {
    public final Coordinates coordinates;
    public final double altitude;

    public Position(Coordinates coordinates, double altitude) {
        this.coordinates = coordinates;
        this.altitude = altitude;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Position position = (Position) o;

        if (Double.compare(altitude, position.altitude) != 0) return false;
        return Objects.equals(coordinates, position.coordinates);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = coordinates != null ? coordinates.hashCode() : 0;
        temp = Double.doubleToLongBits(altitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Position{" +
                "coordinates=" + coordinates +
                ", altitude=" + altitude +
                '}';
    }
}
