package com.sslproxy.coordinator

import munit.FunSuite

class MainSuite extends FunSuite:

  test("database worker permits reserve two pool connections"):
    assertEquals(Main.dbWorkerPermits(10), 8L)
    assertEquals(Main.dbWorkerPermits(4), 2L)

  test("database worker permits retain one worker for small pools"):
    assertEquals(Main.dbWorkerPermits(2), 1L)
    assertEquals(Main.dbWorkerPermits(1), 1L)
