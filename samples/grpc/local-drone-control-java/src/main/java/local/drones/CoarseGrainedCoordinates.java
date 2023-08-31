package local.drones;

public final class CoarseGrainedCoordinates {
    public final double latitude;
    public final double longitude;

    public CoarseGrainedCoordinates(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public static CoarseGrainedCoordinates fromCoordinates(Coordinates location) {
        // not entirely correct, but good enough for a sample/demo
        // 435-1020m precision depending on place on earth
        return new CoarseGrainedCoordinates(
                Math.floor(location.latitude * 100 + 0.5d) / 100,
                Math.floor(location.longitude * 100 + 0.5d) / 100);
    }

    public common.proto.Coordinates toProto() {
        return common.proto.Coordinates.newBuilder()
                .setLatitude(latitude)
                .setLongitude(longitude)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CoarseGrainedCoordinates that = (CoarseGrainedCoordinates) o;

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
        return "CoarseGrainedCoordinates{" +
                "latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
}
