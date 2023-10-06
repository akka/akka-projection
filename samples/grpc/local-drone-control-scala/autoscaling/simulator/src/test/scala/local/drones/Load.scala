package local.drones

import io.gatling.core.Predef._
import com.github.phisgr.gatling.grpc.Predef._
import com.typesafe.config.ConfigFactory
import local.drones.proto.drone_api.DroneServiceGrpc
import local.drones.proto.drone_api.ReportLocationRequest

import scala.concurrent.duration._

class Load extends Simulation {

  val RampTime                   = 5.minutes
  val MaxNumberOfDrones          = 500
  val NumberOfDeliveriesPerDrone = 2

  val Location          = Coordinates(59.33258, 18.0649)
  val StartRadius       = 1000 // metres
  val DestinationRadius = 5000 // metres

  // 2m / 100ms = 72 km/hour
  val ReportEvery    = 100.millis
  val TravelDistance = 2 // metres

  val config = ConfigFactory.load().getConfig("local-drone-control")

  val grpcProtocol = {
    val host = config.getString("host")
    val port = config.getInt("port")
    val tls  = config.getBoolean("tls")
    val channelBuilder = {
      val builder = managedChannelBuilder(name = host, port = port)
      if (tls) builder else builder.usePlaintext()
    }
    grpc(channelBuilder)
  }

  def droneIds = Iterator.from(1).map(id => Map("droneId" -> f"drone-$id%05d"))

  type Path = Iterator[Coordinates]

  def randomDeliveryPath: Path = {
    val start       = Coordinates.random(Location, radiusMetres = StartRadius)
    val destination = Coordinates.random(Location, radiusMetres = DestinationRadius)
    Coordinates.path(start, destination, everyMetres = TravelDistance)
  }

  val reportLocation =
    grpc("report location")
      .rpc(DroneServiceGrpc.METHOD_REPORT_LOCATION)
      .payload(session => {
        val coordinates = session("path").as[Path].next()
        ReportLocationRequest(
          droneId = session("droneId").as[String],
          coordinates = Some(coordinates.toProto),
          altitude = 10
        )
      })

  val updateDrones =
    scenario("update drones")
      .feed(droneIds)
      .repeat(NumberOfDeliveriesPerDrone) {
        exec(session => session.set("path", randomDeliveryPath))
          .doWhile(session => session("path").as[Path].hasNext) {
            pace(ReportEvery).exec(reportLocation)
          }
      }

  setUp(
    updateDrones.inject(rampUsers(MaxNumberOfDrones).during(RampTime))
  ).protocols(grpcProtocol)
}
