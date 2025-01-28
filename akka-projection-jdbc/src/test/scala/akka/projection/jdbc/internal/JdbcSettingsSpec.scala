/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class JdbcSettingsSpec extends TestSuite with Matchers with AnyWordSpecLike with LogCapturing {

  "Loading JdbcSettings" must {

    "accept convert a valid string to integer when reading the pool size" in {

      val config: Config =
        ConfigFactory.parseString("""
          akka {
            loglevel = "DEBUG"
            projection.jdbc {
              dialect = "h2-dialect"
              blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = "5"
            }
          }
          """)

      createSystem(config) { sys =>
        JdbcSettings(sys)
      }
    }

    "accept convert a valid string (with spaces) to integer when reading the pool size" in {

      val config: Config =
        ConfigFactory.parseString("""
          akka {
            loglevel = "DEBUG"
            projection.jdbc {
              dialect = "h2-dialect"
              blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = " 5  "
            }
          }
          """)

      createSystem(config) { sys =>
        JdbcSettings(sys)
      }
    }

    "accept a correctly filled pool size" in {

      val config: Config =
        ConfigFactory.parseString("""
          akka {
            loglevel = "DEBUG"
            projection.jdbc {
              dialect = "h2-dialect"
              blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
            }
          }
          """)

      createSystem(config) { sys =>
        JdbcSettings(sys)
      }
    }

    "throw an exception if pool size is configured with invalid value (unparsable string)" in {

      val config: Config =
        ConfigFactory.parseString("""
          akka {
            loglevel = "DEBUG"
            projection.jdbc {
              dialect = "h2-dialect"
              blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = "this-is-not-valid"
            }
          }
          """)

      createSystem(config) { sys =>
        val msg =
          intercept[IllegalArgumentException] {
            JdbcSettings(sys)
          }.getMessage

        msg should startWith(
          """Value ["this-is-not-valid"] is not a valid value for settings 'akka.projection.jdbc.blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size'""")
      }
    }

    "throw exception when dialect not defined" in {

      val config: Config =
        ConfigFactory.parseString("""
          akka {
            loglevel = "DEBUG"
            projection.jdbc {
              blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
            }
          }
          """)

      createSystem(config) { sys =>
        val msg =
          intercept[IllegalArgumentException] {
            JdbcSettings(sys)
          }.getMessage

        msg shouldBe "Dialect type not set. Settings 'akka.projection.jdbc.dialect' currently set to []"
      }
    }

    "throw exception when dialect is unknown" in {

      val config: Config =
        ConfigFactory.parseString("""
          akka {
            loglevel = "DEBUG"
            projection.jdbc {
              dialect = "h3-dialect"
              blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 5
            }
          }
          """)

      createSystem(config) { sys =>
        val msg =
          intercept[IllegalArgumentException] {
            JdbcSettings(sys)
          }.getMessage

        msg shouldBe "Unknown dialect type: [h3-dialect]. Check settings 'akka.projection.jdbc.dialect'"
      }
    }

    "throw exception when pool size not defined" in {

      val config: Config =
        ConfigFactory.parseString("""
          akka {
            loglevel = "DEBUG"
            projection.jdbc {
              dialect = "h2-dialect"
            }
          }
          """)

      createSystem(config) { sys =>
        intercept[IllegalArgumentException] {
          JdbcSettings(sys)
        }
      }
    }

    "dont' fail if user decide to step out of a thread-pool-executor" in {
      val config: Config =
        ConfigFactory.parseString("""
          akka {
            loglevel = "DEBUG"
            projection.jdbc {
              dialect = "h2-dialect"
              
              # user tweaks the dispatcher to be something else
              # we can't check the pool size if it's not a thread-pool-executor
              blocking-jdbc-dispatcher {
                executor = "fork-join-executor"
                fork-join-executor {
                  parallelism-min = 2
                  parallelism-factor = 2.0
                  parallelism-max = 10
                }
                throughput = 1
              }
            }
          }
          """)

      createSystem(config) { sys =>
        JdbcSettings(sys)
      }

    }
  }

  private def createSystem(config: Config)(func: ActorSystem[Any] => Unit) = {
    val sys = ActorSystem(Behaviors.empty[Any], "test-sys", config)
    func(sys)
    sys.terminate()
    Await.ready(sys.whenTerminated, 3.seconds)
  }
}
