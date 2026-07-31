package com.sslproxy.coordinator.kafka

import munit.FunSuite

class WirelessConsumerServiceSuite extends FunSuite:

  // ========== extractField ==========

  test("extractField returns Some for present non-empty string field"):
    val json = """{"mac": "aa:bb:cc:dd:ee:ff", "reply_topic": "wireless.mac.lookup.reply"}"""
    assertEquals(WirelessConsumerService.extractField(json, "mac"), Some("aa:bb:cc:dd:ee:ff"))
    assertEquals(WirelessConsumerService.extractField(json, "reply_topic"), Some("wireless.mac.lookup.reply"))

  test("extractField returns None for absent field"):
    assertEquals(WirelessConsumerService.extractField("{}", "mac"), None)

  test("extractField returns None for empty string field"):
    assertEquals(WirelessConsumerService.extractField("""{"mac": ""}""", "mac"), None)

  test("extractField returns None for non-string field"):
    assertEquals(WirelessConsumerService.extractField("""{"mac": 42}""", "mac"), None)

  test("extractField returns None for malformed JSON"):
    assertEquals(WirelessConsumerService.extractField("not json", "mac"), None)

  // ========== isValidKafkaTopic ==========

  test("isValidKafkaTopic accepts valid topic"):
    assert(WirelessConsumerService.isValidKafkaTopic("wireless.mac.lookup.reply"))
    assert(WirelessConsumerService.isValidKafkaTopic("_INBOX.atheros_sensor.12345.7"))
    assert(WirelessConsumerService.isValidKafkaTopic("topic-with-dashes"))
    assert(WirelessConsumerService.isValidKafkaTopic("topic_with_underscores"))

  test("isValidKafkaTopic rejects single dot"):
    assert(!WirelessConsumerService.isValidKafkaTopic("."))

  test("isValidKafkaTopic rejects double dot"):
    assert(!WirelessConsumerService.isValidKafkaTopic(".."))

  test("isValidKafkaTopic rejects topic with invalid characters"):
    assert(!WirelessConsumerService.isValidKafkaTopic("bad?topic"))
    assert(!WirelessConsumerService.isValidKafkaTopic("topic with spaces"))
    assert(!WirelessConsumerService.isValidKafkaTopic("topic,with,commas"))

  // ========== isAllowedReplyTopic ==========

  test("isAllowedReplyTopic accepts configured reply topics"):
    assert(WirelessConsumerService.isAllowedReplyTopic("wireless.mac.lookup.reply"))
    assert(WirelessConsumerService.isAllowedReplyTopic("wireless.networks.authorized.reply"))

  test("isAllowedReplyTopic accepts sensor inbox prefix"):
    assert(WirelessConsumerService.isAllowedReplyTopic("_INBOX.atheros_sensor.12345.7"))
    assert(WirelessConsumerService.isAllowedReplyTopic("_INBOX.atheros_sensor."))

  test("isAllowedReplyTopic rejects unknown topic"):
    assert(!WirelessConsumerService.isAllowedReplyTopic("wireless.attacker.reply"))
    assert(!WirelessConsumerService.isAllowedReplyTopic("_INBOX.other_sensor.123"))

  // ========== resolveReplyTopic ==========

  test("resolveReplyTopic uses reply_topic from payload when valid"):
    val json = """{"reply_topic": "wireless.mac.lookup.reply"}"""
    assertEquals(
      WirelessConsumerService.resolveReplyTopic(json, "wireless.default.reply"),
      "wireless.mac.lookup.reply"
    )

  test("resolveReplyTopic falls back to default for invalid reply_topic"):
    val json = """{"reply_topic": "bad?topic"}"""
    assertEquals(
      WirelessConsumerService.resolveReplyTopic(json, "wireless.default.reply"),
      "wireless.default.reply"
    )

  test("resolveReplyTopic falls back to default for unapproved reply_topic"):
    val json = """{"reply_topic": "wireless.unknown.reply"}"""
    assertEquals(
      WirelessConsumerService.resolveReplyTopic(json, "wireless.default.reply"),
      "wireless.default.reply"
    )

  test("resolveReplyTopic accepts sensor inbox as reply topic"):
    val json = """{"reply_topic": "_INBOX.atheros_sensor.abc123.7"}"""
    assertEquals(
      WirelessConsumerService.resolveReplyTopic(json, "wireless.default.reply"),
      "_INBOX.atheros_sensor.abc123.7"
    )

  test("resolveReplyTopic falls back to default when no reply_topic field"):
    assertEquals(
      WirelessConsumerService.resolveReplyTopic("{}", "wireless.default.reply"),
      "wireless.default.reply"
    )

  test("resolveReplyTopic falls back to default for empty reply_topic"):
    assertEquals(
      WirelessConsumerService.resolveReplyTopic("""{"reply_topic": ""}""", "wireless.default.reply"),
      "wireless.default.reply"
    )

  test("resolveReplyTopic falls back to default for malformed JSON"):
    assertEquals(
      WirelessConsumerService.resolveReplyTopic("not json", "wireless.default.reply"),
      "wireless.default.reply"
    )

  // ========== hashMac ==========

  test("hashMac produces hashed output for valid MAC"):
    val hashed = WirelessConsumerService.hashMac("aa:bb:cc:dd:ee:ff")
    assert(hashed.exists(_.matches("[0-9a-f]{24}")), s"expected truncated SHA-256, got $hashed")
    assertEquals(hashed, WirelessConsumerService.hashMac("AA:BB:CC:DD:EE:FF"))

  test("hashMac omits invalid null input"):
    assertEquals(WirelessConsumerService.hashMac(null), None)

  test("hashMac omits malformed input"):
    assertEquals(WirelessConsumerService.hashMac("ab"), None)
