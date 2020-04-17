/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal.spi;

import akka.actor.ClassicActorSystemProvider;
import akka.projection.ProjectionId;

public interface ProjectionMetrics {

    /**
     * Must be invoked for each element received from the Source when the Source provides it. If possible,
     * invoke this method in a stream stage as close to the stream stage where you read from the wire to
     * measure the parsing, deserializing and other steps of the processing prior to the event handling.
     *
     * @param projectionId      the projection id
     * @param creationTimestamp if the element traversing the stream contains the creation time, provide it. Set
     *                          to null if the information is not available.
     * @param systemProvider    a `ClassicActorSystemProvider` for telemetry to extract/set data on the ActorSystem
     * @return a contextual object. This context must propagate with the elt.
     */
    Object onProcessStart(ProjectionId projectionId,
                          Long creationTimestamp,
                          ClassicActorSystemProvider systemProvider);

    /**
     * Must be invoked for each element processed successfully only when the associated offset has been committed.
     *
     * @param projectionId the projection id
     * @param context      the contextual object returned by `onProcessStart`
     * @return a contextual object. The returned instance may not be the received one.
     */
    Object onProcessComplete(ProjectionId projectionId,
                             Object context);

    /**
     * Must be invoked when the stream fails.
     *
     * @param projectionId   the projection id
     * @param cause          the cause of the failure
     * @param systemProvider a `ClassicActorSystemProvider` for telemetry to extract/set data on the ActorSystem
     */
    void onFailure(ProjectionId projectionId,
                   Throwable cause,
                   ClassicActorSystemProvider systemProvider);

}
