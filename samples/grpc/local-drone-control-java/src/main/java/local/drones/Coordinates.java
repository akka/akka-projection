package local.drones;

/**
 * Decimal degree coordinates
 */
public final class Coordinates {

    private static final int ROUGHLY_RADIUS_OF_EARTH_IN_M = 6371000;
    public final double latitude;
    public final double longitude;

    public Coordinates(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public long distanceTo(Coordinates other) {
        // using the haversine formula https://en.wikipedia.org/wiki/Versine#hav
        var latitudeDistance = Math.toRadians(latitude - other.latitude);
        var longitudeDistance = Math.toRadians(longitude - other.longitude);
        var sinLatitude = Math.sin(latitudeDistance / 2);
        var sinLongitude = Math.sin(longitudeDistance / 2);
        var a = sinLatitude * sinLatitude +
                (Math.cos(Math.toRadians(latitude)) *
                        Math.cos(Math.toRadians(other.latitude)) *
                        sinLongitude * sinLongitude);
        var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return (long) (ROUGHLY_RADIUS_OF_EARTH_IN_M * c);
    }

    public common.proto.Coordinates toProto() {
        return common.proto.Coordinates.newBuilder()
                .setLatitude(latitude)
                .setLongitude(longitude)
                .build();
    }

    public static Coordinates fromProto(common.proto.Coordinates pc) {
        return new Coordinates(pc.getLatitude(), pc.getLongitude());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Coordinates that = (Coordinates) o;

        if (Double.compare(latitude, that.latitude) != 0) return false;
        return Double.compare(longitude, that.longitude) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(latitude);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(longitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Coordinates{" +
                "latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
}