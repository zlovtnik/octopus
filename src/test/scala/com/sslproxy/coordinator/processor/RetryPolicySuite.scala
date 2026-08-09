package com.sslproxy.coordinator.processor

import munit.FunSuite

import scala.concurrent.duration.*

class RetryPolicySuite extends FunSuite:
  test("retry delays are deterministic, capped, and bounded by jitter") {
    val policy = RetryPolicy(1.second, 8.seconds, Some(5))
    val delays = (1 to 20).map(policy.delay(ProcessorId.EventRetention, _))
    assertEquals(delays, (1 to 20).map(policy.delay(ProcessorId.EventRetention, _)))
    assert(delays.forall(_ <= 9600.millis))
    assert(delays.forall(_ >= 800.millis))
  }

  test("retry attempt limit is pure and inclusive") {
    val policy = RetryPolicy(1.second, 8.seconds, Some(3))
    assert(policy.permits(3))
    assert(!policy.permits(4))
  }
