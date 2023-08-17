/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package drones

import akka.Done
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.grpc.GrpcServiceException
import akka.util.Timeout
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.slf4j.LoggerFactory
import drones.proto

import scala.concurrent.Future
import scala.concurrent.TimeoutException

class DroneServiceImpl(system: ActorSystem[_]) extends proto.DroneService {

  import system.executionContext

  private val logger = LoggerFactory.getLogger(getClass)

  implicit private val timeout: Timeout =
    Timeout.create(
      system.settings.config
        .getDuration("local-drone-control.drone-service.ask-timeout"))

  private val sharding = ClusterSharding(system)

  override def reportLocation(
      in: proto.ReportLocationRequest): Future[Empty] = {
    logger.info(
      "Report location ({},{},{}) for drone {}",
      in.latitude,
      in.longitude,
      in.altitude,
      in.droneId)
    val entityRef = sharding.entityRefFor(Drone.EntityKey, in.droneId)
    val reply: Future[Done] = entityRef.ask(
      Drone.ReportPosition(
        Position(Coordinates(in.latitude, in.longitude), in.altitude),
        _))
    val response = reply.map(_ => Empty.defaultInstance)
    convertError(response)
  }

  private def convertError[T](response: Future[T]): Future[T] = {
    response.recoverWith {
      case _: TimeoutException =>
        Future.failed(
          new GrpcServiceException(
            Status.UNAVAILABLE.withDescription("Operation timed out")))
      case exc =>
        Future.failed(
          new GrpcServiceException(
            Status.INVALID_ARGUMENT.withDescription(exc.getMessage)))
    }
  }
}
