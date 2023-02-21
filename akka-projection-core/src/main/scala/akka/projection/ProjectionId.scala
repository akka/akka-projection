/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.collection.immutable

import akka.util.ccompat.JavaConverters._

object ProjectionId {

  /**
   * Constructs a ProjectionId.
   *
   * A ProjectionId is composed by a name and a key.
   *
   * The projection name is shared across multiple instances of [[Projection]] with different keys.
   * For example, a "user-view" could be the name of a projection.
   *
   * The key must be unique for a projection name.
   * For example, a "user-view" could have multiple projections with different keys representing different partitions,
   * shards, etc.
   *
   * @param name - the projection name
   * @param key  - the unique key. The key must be unique for a projection name.
   * @return ProjectionId
   */
  def apply(name: String, key: String): ProjectionId = new ProjectionId(name, key)

  /**
   * Java API: Constructs a ProjectionId.
   *
   * A ProjectionId is composed by a name and a key.
   *
   * The projection name is shared across multiple instances of [[Projection]] with different keys.
   * For example, a "user-view" could be the name of a projection.
   *
   * The key must be unique for a projection name.
   * For example, a "user-view" could have multiple projections with different keys representing different partitions,
   * shards, etc.
   *
   * @param name - the projection name
   * @param key  - the unique key. The key must be unique for a projection name.
   * @return a ProjectionId
   */
  def of(name: String, key: String): ProjectionId = apply(name, key)

  /**
   * Constructs a Set of ProjectionId.
   *
   * A ProjectionId is composed by a name and a key.
   *
   * The projection name is shared across multiple instances of [[Projection]] with different keys.
   * For example, a "user-view" could be the name of a projection.
   *
   * The key must be unique for a projection name.
   * For example, a "user-view" could have multiple projections with different keys representing different partitions,
   * shards, etc.
   *
   * @param name - the projection name
   * @param keys  - the Set of keys to associated with the passed name.
   * @return an [[immutable.Set]] of [[ProjectionId]]s
   */
  def apply(name: String, keys: immutable.Set[String]): immutable.Set[ProjectionId] =
    keys.map(key => new ProjectionId(name, key))

  /**
   * Java API: Constructs a Set of ProjectionId.
   *
   * A ProjectionId is composed by a name and a key.
   *
   * The projection name is shared across multiple instances of [[Projection]] with different keys.
   * For example, a "user-view" could be the name of a projection.
   *
   * The key must be unique for a projection name.
   * For example, a "user-view" could have multiple projections with different keys representing different partitions,
   * shards, etc.
   *
   * @param name - the projection name
   * @param keys  - the Set of keys to associated with the passed name.
   * @return an [[java.util.Set]] of [[ProjectionId]]s
   */
  def of(name: String, keys: java.util.Set[String]): java.util.Set[ProjectionId] =
    keys.asScala.map { (key: String) => new ProjectionId(name, key) }.asJava
}

final class ProjectionId private (val name: String, val key: String) {

  require(name != null, "name must not be null")
  require(name.trim.nonEmpty, "name must not be empty")
  require(key != null, "key must not be null")
  require(key.trim.nonEmpty, "key must not be empty")

  /**
   * The unique id formed by the concatenation of `name` and `key`.
   * A dash (-) is used as separator.
   */
  val id = s"$name-$key"

  override def toString: String = s"ProjectionId($name, $key)"

  override def equals(other: Any): Boolean = other match {
    case that: ProjectionId => id == that.id
    case _                  => false
  }

  override def hashCode(): Int = id.hashCode
}
