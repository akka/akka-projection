/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import scala.annotation.tailrec

import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TopicMatcher {
  val Separator = '/'
  val SingleLevelWildcard = '+'
  val SingleLevelWildcardStr = "+"
  val MultiLevelWildcard = '#'
  val MultiLevelWildcardStr = "#"

  def checkValid(expression: String): Unit = {
    if (expression.isEmpty)
      throw new IllegalArgumentException("Empty topic expression is not allowed")

    // single level
    val i = expression.indexOf(SingleLevelWildcard)
    if (i > 0) {
      if (expression.length >= 2 && expression.charAt(i - 1) != Separator)
        throw new IllegalArgumentException(
          s"Single level wildcard $SingleLevelWildcard must be a full token, expression [$expression]")
      if (i <= expression.length - 2 && expression.charAt(i + 1) != Separator)
        throw new IllegalArgumentException(
          s"Single level wildcard $SingleLevelWildcard must be a full token, expression [$expression]")
    }

    // multi level
    val j = expression.indexOf(MultiLevelWildcard)
    if (j > 0) {
      if (j != expression.length - 1)
        throw new IllegalArgumentException(
          s"Multi level wildcard $MultiLevelWildcard must be at the end, expression [$expression]")
      if (expression.length >= 2 && expression.charAt(j - 1) != Separator)
        throw new IllegalArgumentException(
          s"Multi level wildcard $MultiLevelWildcard must be a full token, expression [$expression]")
    }

  }

  def apply(expression: String): TopicMatcher = {
    checkValid(expression)

    if (expression.length == 1 && expression.charAt(0) == MultiLevelWildcard)
      AllTopicMatcher
    else if (expression.indexOf(SingleLevelWildcard) >= 0 || expression.indexOf(MultiLevelWildcard) >= 0)
      new WildcardMatcher(expression)
    else
      new ExactTopicMatcher(expression)

  }

  def stripEnd(str: String, stripChar: Character): String = {
    @tailrec def loop(i: Int): Int = {
      if (i < 0 || str.charAt(i) != stripChar)
        i
      else
        loop(i - 1)
    }
    val i = loop(str.length - 1)

    if (i == str.length - 1 || i < 0)
      str
    else
      str.substring(0, i + 1)
  }
}

/** INTERNAL API */
@InternalApi private[akka] sealed trait TopicMatcher {
  def matches(topic: String): Boolean

  def matches(env: EventEnvelope[_], topicTagPrefix: String): Boolean = {
    if (env.tags.isEmpty)
      false
    else {
      env.tags.iterator.exists { tag =>
        if (tag.startsWith(topicTagPrefix))
          matches(tag.substring(topicTagPrefix.length))
        else
          false
      }
    }
  }
}

/** INTERNAL API */
@InternalApi object AllTopicMatcher extends TopicMatcher {
  override def matches(topic: String): Boolean =
    true

  override def matches(env: EventEnvelope[_], topicTagPrefix: String): Boolean =
    true // also true if no topic tags in EventEnvelope
}

/** INTERNAL API */
@InternalApi final class ExactTopicMatcher(expression: String) extends TopicMatcher {
  override def matches(topic: String): Boolean =
    expression == topic
}

/** INTERNAL API */
@InternalApi final class WildcardMatcher(expression: String) extends TopicMatcher {
  import TopicMatcher._
  private val expressionTokens = expression.split(Separator)

  override def matches(topic: String): Boolean = {
    // inspiration from https://github.com/hivemq/hivemq-community-edition/blob/master/src/main/java/com/hivemq/mqtt/topic/TokenizedTopicMatcher.java
    val strippedTopic = stripEnd(topic, Separator)
    val topicTokens = strippedTopic.split(Separator)
    val smallest = math.min(topicTokens.length, expressionTokens.length)

    @tailrec def loop(i: Int): Boolean = {
      if (i == smallest)
        // it's a match if the length is equal or the expression token with the number x+1
        // (where x is the topic length) is a wildcard
        expressionTokens.length == topicTokens.length ||
        (topicTokens.length - strippedTopic.length == 1 && expressionTokens(expressionTokens.length - 1) == MultiLevelWildcardStr)
      else if (expressionTokens(i) == topicTokens(i))
        loop(i + 1)
      else if (expressionTokens(i) == MultiLevelWildcardStr)
        true
      else if (expressionTokens(i) == SingleLevelWildcardStr)
        // matches topic level wildcard, so we can continue
        loop(i + 1)
      else
        // does not match a wildcard and is not equal to the topic token
        false
    }

    loop(0)
  }

}
