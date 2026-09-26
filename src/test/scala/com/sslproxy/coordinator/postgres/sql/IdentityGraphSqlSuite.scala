package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

import java.nio.file.{Files, Paths}

class IdentityGraphSqlSuite extends FunSuite:
  test("graph upserts qualify retained target columns"):
    val implementation = Files.readString(
      Paths.get("src/main/scala/com/sslproxy/coordinator/postgres/sql/IdentityGraphSql.scala")
    )

    assert(implementation.contains("graph_nodes.observed_at"))
    assert(implementation.contains("graph_edges.observed_at"))
    assert(implementation.contains("graph_nodes.location_id"))
    assert(!implementation.contains("COALESCE(observed_at, EXCLUDED.observed_at)"))
    assert(!implementation.contains("COALESCE(EXCLUDED.observed_at, observed_at)"))

  test("graph edge writes record a documented weight basis"):
    val implementation = Files.readString(
      Paths.get("src/main/scala/com/sslproxy/coordinator/postgres/sql/IdentityGraphSql.scala")
    )

    assert(implementation.contains("'observed_at', COUNT(*), 'frame_count'"))
    assert(implementation.contains("'identity_member', member.confidence, 'cluster_confidence'"))
    assert(implementation.contains("'roaming', pair.shared_aps::double precision, 'time_overlap_windows'"))
    assert(implementation.contains("'same_channel', pair.shared_channels::double precision, 'channel_overlap'"))
    assert(implementation.contains("'vendor_link', pair.shared_ouis::double precision, 'vendor_match'"))

  test("device pair edge ids use canonical mac ordering"):
    val implementation = Files.readString(
      Paths.get("src/main/scala/com/sslproxy/coordinator/postgres/sql/IdentityGraphSql.scala")
    )

    val pairEdges = implementation
      .split("\n")
      .count(line =>
        line.contains("LEAST(left_frame.source_mac, right_frame.source_mac)") ||
          line.contains("LEAST(left_oui.source_mac, right_oui.source_mac)")
      )
    assert(pairEdges >= 4)
    assert(implementation.contains("CONCAT('roaming:', pair.left_mac, ':', pair.right_mac)"))
    assert(implementation.contains("CONCAT('same-channel:', pair.left_mac, ':', pair.right_mac)"))
    assert(implementation.contains("CONCAT('vendor-link:', pair.left_mac, ':', pair.right_mac)"))

  test("automatic identity confirmation is guarded and pending candidates are not exposed"):
    val implementation = Files.readString(
      Paths.get("src/main/scala/com/sslproxy/coordinator/postgres/sql/IdentityGraphSql.scala")
    )

    assert(implementation.contains("AutomaticMergeSimilarity = 0.98d"))
    assert(implementation.contains("left_device.registered_device_id = right_device.registered_device_id"))
    assert(implementation.contains("candidate.trusted_registered_device_id IS NOT NULL"))
    assert(implementation.contains("candidate.confirmation_source = 'automatic'"))
    assert(implementation.contains("decision.candidate_id IS NULL"))
    assert(implementation.contains("WHERE candidate.status = 'confirmed'"))
    assert(implementation.contains("'same_device', candidate.confidence"))
    assert(!implementation.contains("'same_device', pair.cosine_similarity"))
