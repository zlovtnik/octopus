package com.sslproxy.coordinator.postgres

import cats.effect.IO
import com.sslproxy.coordinator.config.AppConfig
import com.zaxxer.hikari.HikariDataSource
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.concurrent.atomic.AtomicInteger
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class PostgresSinkRetrySuite extends CatsEffectSuite:
  private def proxy[A](kind: Class[A])(run: (String, Array[AnyRef]) => AnyRef): A =
    kind.cast(Proxy.newProxyInstance(kind.getClassLoader, Array(kind), new InvocationHandler:
      def invoke(target: Any, method: Method, args: Array[AnyRef]): AnyRef =
        run(method.getName, Option(args).getOrElse(Array.empty[AnyRef]))
    ))

  private class Pool(brokenCleanup: Boolean = false) extends HikariDataSource:
    val acquisitions = new AtomicInteger()
    val active = new AtomicInteger()
    val evictions = new AtomicInteger()
    val restores = new AtomicInteger()
    override def getConnection(): Connection =
      val number = acquisitions.incrementAndGet()
      active.incrementAndGet(): Unit
      val result = proxy(classOf[ResultSet])((_, _) => null)
      val statement = proxy(classOf[PreparedStatement]) { (name, args) =>
        name match
          case "executeQuery" => result
          case "setString" => assertEquals(args(1), "30000ms"); null
          case _ => null
      }
      proxy(classOf[Connection]) { (name, args) =>
        name match
          case "getNetworkTimeout" => Integer.valueOf(1234)
          case "isClosed" => java.lang.Boolean.FALSE
          case "setNetworkTimeout" =>
            if args(1) == Integer.valueOf(1234) then restores.incrementAndGet(): Unit
            null
          case "prepareStatement" =>
            assertEquals(args(0), "SELECT set_config('statement_timeout', ?, true)")
            statement
          case "rollback" if brokenCleanup && number == 1 => throw SQLException("cleanup-secret", "08006")
          case "close" => active.decrementAndGet(); null
          case _ => null
      }
    override def evictConnection(connection: Connection): Unit =
      evictions.incrementAndGet(): Unit

  test("recovery releases connections throughout the 200 ms backoff and restores timeout"):
    val pool = new Pool()
    val sink = PostgresTransactor.fromDataSource(pool, AppConfig.load.postgres)
    val calls = new AtomicInteger()
    val work = sink.withTransactionRetry("test_recovery") { _ =>
      if calls.incrementAndGet() == 1 then throw SQLException("cancel", "57014")
      42
    }
    for
      fiber <- work.start
      _ <- (IO.sleep(100.millis) *> IO(assertEquals(pool.active.get(), 0))).guaranteeCase {
        case cats.effect.kernel.Outcome.Succeeded(_) => IO.unit
        case _ => fiber.cancel
      }
      value <- fiber.joinWithNever.guarantee(fiber.cancel)
    yield
      assertEquals(value, 42)
      assertEquals(pool.acquisitions.get(), 2)
      assertEquals(pool.restores.get(), 2)

  test("exhaustion is exactly three attempts and retains original network failure despite rollback failure"):
    val pool = new Pool(brokenCleanup = true)
    val sink = PostgresTransactor.fromDataSource(pool, AppConfig.load.postgres)
    val original = SQLException("network-secret", "08006")
    sink.withTransactionRetry("test_exhausted")(_ => throw original).attempt.map { result =>
      assertEquals(result, Left(original))
      assertEquals(pool.acquisitions.get(), 3)
      assertEquals(pool.active.get(), 0)
      assertEquals(pool.evictions.get(), 1)
      assertEquals(original.getSuppressed.length, 1)
    }

  test("permanent SQL failure containing timeout does not retry"):
    val pool = new Pool()
    val sink = PostgresTransactor.fromDataSource(pool, AppConfig.load.postgres)
    sink.withTransactionRetry("test_permanent")(_ => throw SQLException("timeout-value", "23505")).attempt.map { result =>
      assert(result.isLeft)
      assertEquals(pool.acquisitions.get(), 1)
      assertEquals(pool.active.get(), 0)
    }

  test("broken network connection is evicted and next attempt acquires another connection"):
    val pool = new Pool(brokenCleanup = true)
    val sink = PostgresTransactor.fromDataSource(pool, AppConfig.load.postgres)
    sink.withTransactionRetry("test_network_recovery") { _ =>
      if pool.acquisitions.get() == 1 then throw SQLException("network-value", "08006")
      1
    }.map { value =>
      assertEquals(value, 1)
      assertEquals(pool.evictions.get(), 1)
      assertEquals(pool.acquisitions.get(), 2)
    }
