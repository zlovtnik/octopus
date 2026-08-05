package com.sslproxy.coordinator.kafka

import munit.FunSuite

class WirelessConsumerServiceSuite extends FunSuite:
  private val ConfiguredReplyTopics = Set(
    "wireless.backlog.list.reply",
    "wireless.backlog.prune.reply",
    "wireless.mac.lookup.reply",
    "wireless.networks.authorized.reply"
  )

  test("wireless backlog identity requires non-empty dedupe and stream keys") {
    assert(WirelessConsumerService.parseBacklogIdentity("{}").isLeft)
    assert(WirelessConsumerService.parseBacklogIdentity(
      """{"dedupe_key":"","stream_name":"wireless.audit"}"""
    ).isLeft)
    val parsed = WirelessConsumerService.parseBacklogIdentity(
      """{"dedupe_key":"frame-1","stream_name":"wireless.audit","payload":{"x":1}}"""
    )
    assertEquals(parsed.map(value => (value._2, value._3)), Right(("frame-1", "wireless.audit")))
  }

  test("backlog reply topics use the same allowlist as current wireless replies") {
    assert(WirelessConsumerService.isAllowedReplyTopic("wireless.backlog.list.reply", ConfiguredReplyTopics))
    assert(WirelessConsumerService.isAllowedReplyTopic("wireless.backlog.prune.reply", ConfiguredReplyTopics))
  }

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
    assert(WirelessConsumerService.isAllowedReplyTopic("wireless.mac.lookup.reply", ConfiguredReplyTopics))
    assert(WirelessConsumerService.isAllowedReplyTopic("wireless.networks.authorized.reply", ConfiguredReplyTopics))
    assert(WirelessConsumerService.isAllowedReplyTopic("wireless.custom.reply", Set("wireless.custom.reply")))
    assert(!WirelessConsumerService.isAllowedReplyTopic("wireless.mac.lookup.reply", Set("wireless.custom.reply")))

  test("isAllowedReplyTopic accepts sensor inbox prefix"):
    assert(WirelessConsumerService.isAllowedReplyTopic("_INBOX.atheros_sensor.12345.7", ConfiguredReplyTopics))
    assert(WirelessConsumerService.isAllowedReplyTopic("_INBOX.atheros_sensor.", ConfiguredReplyTopics))

  test("isAllowedReplyTopic rejects unknown topic"):
    assert(!WirelessConsumerService.isAllowedReplyTopic("wireless.attacker.reply", ConfiguredReplyTopics))
    assert(!WirelessConsumerService.isAllowedReplyTopic("_INBOX.other_sensor.123", ConfiguredReplyTopics))

  // ========== resolveReplyTopic ==========

  test("resolveReplyTopic uses reply_topic from payload when valid"):
    val json = """{"reply_topic": "wireless.mac.lookup.reply"}"""
    assertEquals(
      WirelessConsumerService.resolveReplyTopic(json, "wireless.default.reply", Set("wireless.mac.lookup.reply")),
      "wireless.mac.lookup.reply"
    )

  test("resolveReplyTopic falls back to default for invalid reply_topic"):
    val json = """{"reply_topic": "bad?topic"}"""
    assertEquals(
      WirelessConsumerService.resolveReplyTopic(json, "wireless.default.reply", Set.empty),
      "wireless.default.reply"
    )

  test("resolveReplyTopic falls back to default for unapproved reply_topic"):
    val json = """{"reply_topic": "wireless.unknown.reply"}"""
    assertEquals(
      WirelessConsumerService.resolveReplyTopic(json, "wireless.default.reply", Set.empty),
      "wireless.default.reply"
    )

  test("resolveReplyTopic accepts sensor inbox as reply topic"):
    val json = """{"reply_topic": "_INBOX.atheros_sensor.abc123.7"}"""
    assertEquals(
      WirelessConsumerService.resolveReplyTopic(json, "wireless.default.reply", Set.empty),
      "_INBOX.atheros_sensor.abc123.7"
    )

  test("resolveReplyTopic falls back to default when no reply_topic field"):
    assertEquals(
      WirelessConsumerService.resolveReplyTopic("{}", "wireless.default.reply", Set.empty),
      "wireless.default.reply"
    )

  test("resolveReplyTopic falls back to default for empty reply_topic"):
    assertEquals(
      WirelessConsumerService.resolveReplyTopic("""{"reply_topic": ""}""", "wireless.default.reply", Set.empty),
      "wireless.default.reply"
    )

  test("resolveReplyTopic falls back to default for malformed JSON"):
    assertEquals(
      WirelessConsumerService.resolveReplyTopic("not json", "wireless.default.reply", Set.empty),
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

  test("normalizeMac validates and canonicalizes lookup values"):
    assertEquals(
      WirelessConsumerService.normalizeMac("  AA:BB:CC:DD:EE:FF  "),
      Some("aa:bb:cc:dd:ee:ff")
    )
    assertEquals(WirelessConsumerService.normalizeMac("aa-bb-cc-dd-ee-ff"), None)
