package com.sslproxy.coordinator.tidb.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.processor.{IdentityClusterProjection, ProjectionFunctions}
import doobie.ConnectionIO
import doobie.implicits.*
import io.circe.syntax.*

object IdentityGraphSql:
  def similarityEdges(minimumSimilarity: Double, limit: Int): doobie.Query0[(String, String, Double)] =
    sql"""SELECT left_source_mac, right_source_mac, cosine_similarity
           FROM atheros_search.similarity_pairs
           WHERE left_source_mac IS NOT NULL
             AND right_source_mac IS NOT NULL
             AND left_source_mac <> right_source_mac
             AND cosine_similarity >= ${minimumSimilarity.max(-1.0d).min(1.0d)}
           ORDER BY computed_at, pair_id
           LIMIT ${limit.max(1)}""".query[(String, String, Double)]

  def approvedIdentityEdges(limit: Int): doobie.Query0[(String, String, Double)] =
    sql"""SELECT candidate.mac_a, candidate.mac_b, candidate.confidence
           FROM atheros_search.merge_candidates candidate
           JOIN atheros_search.merge_decisions decision
             ON decision.candidate_id = candidate.candidate_id
           WHERE decision.decision = 'merge'
           ORDER BY decision.decided_at, candidate.candidate_id
           LIMIT ${limit.max(1)}""".query[(String, String, Double)]

  def persistMergeCandidate(left: String, right: String, confidence: Double): ConnectionIO[Int] =
    val ordered = Vector(left, right).sorted
    val candidateId = ProjectionFunctions.stableId("merge-candidate", ordered)
    val runId = ProjectionFunctions.stableId("merge-candidate-run", Vector(candidateId))
    sql"""INSERT INTO atheros_search.merge_candidates (
             candidate_id, mac_a, mac_b, confidence, computed_at, status,
             evidence, expires_at, projection_run_id
           ) VALUES (
             $candidateId, ${ordered.head}, ${ordered.last}, ${confidence.max(0.0d).min(1.0d)},
             CURRENT_TIMESTAMP(6), 'pending',
             JSON_OBJECT('source', 'similarity_pairs'),
             TIMESTAMPADD(DAY, 30, CURRENT_TIMESTAMP(6)), $runId
           ) ON DUPLICATE KEY UPDATE
             confidence = GREATEST(confidence, VALUES(confidence)),
             computed_at = VALUES(computed_at),
             expires_at = VALUES(expires_at),
             projection_run_id = VALUES(projection_run_id),
             updated_at = CURRENT_TIMESTAMP(6)""".update.run

  def persistCluster(value: IdentityClusterProjection): ConnectionIO[Int] =
    if value.members.isEmpty then 0.pure[ConnectionIO]
    else {
    val memberClause = value.members.map(member => fr0"$member").intercalate(fr",")
    val firstSeen = (fr"""SELECT MIN(first_seen), MAX(last_seen)
                            FROM devices
                            WHERE mac_id IN (""" ++ memberClause ++ fr")")
      .query[(Option[java.sql.Timestamp], Option[java.sql.Timestamp])]
      .unique

    firstSeen.flatMap { case (minimum, maximum) =>
      def persistRows(min: java.sql.Timestamp, max: java.sql.Timestamp): ConnectionIO[Int] =
        for
          cluster <- sql"""INSERT INTO atheros_search.identity_clusters (
                            cluster_id, cluster_name, cluster_size, first_seen, last_seen,
                            status, projection_run_id
                          ) VALUES (
                            ${value.clusterId}, ${Some(s"identity-${value.clusterId.take(8)}")},
                            ${value.members.size}, $min, $max,
                            'active', ${value.projectionRunId}
                          ) ON DUPLICATE KEY UPDATE
                            cluster_size = VALUES(cluster_size),
                            first_seen = LEAST(first_seen, VALUES(first_seen)),
                            last_seen = GREATEST(last_seen, VALUES(last_seen)),
                            status = 'active',
                            projection_run_id = VALUES(projection_run_id),
                            updated_at = CURRENT_TIMESTAMP(6)""".update.run
          members <- value.members.traverse { mac =>
            sql"""INSERT INTO atheros_search.identity_cluster_members (
                   cluster_id, mac, confidence, evidence, first_seen, last_seen
                 )
                 SELECT ${value.clusterId}, device.mac_id, ${value.confidence},
                        JSON_OBJECT('source', 'approved_merge_decision'),
                        device.first_seen, device.last_seen
                 FROM devices device
                 WHERE device.mac_id = $mac
                 ON DUPLICATE KEY UPDATE
                   cluster_id = VALUES(cluster_id),
                   confidence = VALUES(confidence),
                   evidence = VALUES(evidence),
                   first_seen = LEAST(first_seen, VALUES(first_seen)),
                   last_seen = GREATEST(last_seen, VALUES(last_seen)),
                   updated_at = CURRENT_TIMESTAMP(6)""".update.run
          }
          inventory <- value.members.traverse { mac =>
            sql"""INSERT INTO atheros_search.inventory_devices (
                   mac, display_name, location_id, first_registered, last_seen,
                   active, registered, tags, similarity_cluster_id,
                   dedup_confidence, known_macs, projection_run_id
                 )
                 SELECT device.mac_id, device.display_name, NULL, device.first_seen, device.last_seen,
                        1, 0, JSON_ARRAY(), ${value.clusterId}, ${value.confidence},
                        CAST(${value.members.asJson.noSpaces} AS JSON), ${value.projectionRunId}
                 FROM devices device
                 WHERE device.mac_id = $mac
                 ON DUPLICATE KEY UPDATE
                   display_name = COALESCE(VALUES(display_name), display_name),
                   last_seen = GREATEST(last_seen, VALUES(last_seen)),
                   similarity_cluster_id = VALUES(similarity_cluster_id),
                   dedup_confidence = VALUES(dedup_confidence),
                   known_macs = VALUES(known_macs),
                   projection_run_id = VALUES(projection_run_id),
                   updated_at = CURRENT_TIMESTAMP(6)""".update.run
          }
        yield cluster + members.sum + inventory.sum

      (minimum, maximum) match
        case (Some(min), Some(max)) => persistRows(min, max)
        case (Some(min), None)      => persistRows(min, min)
        case (None, Some(max))      => persistRows(max, max)
        case (None, None)           => 0.pure[ConnectionIO]
    }
    }

  def projectGraph(limit: Int, projectionRunId: String): ConnectionIO[Int] =
    val batchLimit = limit.max(1)
    for
      deviceNodes <- sql"""INSERT INTO atheros_search.graph_nodes (
                            node_id, node_kind, label, node_payload, location_id,
                            normalized_mac, is_threat, observed_at, projection_run_id
                          )
                          SELECT CONCAT('device:', device.mac_id), 'device', device.display_name,
                                 JSON_OBJECT('mac', device.mac_id), NULL, device.mac_id, 0,
                                 device.last_seen,
                                 $projectionRunId
                          FROM devices device
                          ORDER BY device.last_seen DESC, device.mac_id
                          LIMIT $batchLimit
                    ON DUPLICATE KEY UPDATE
                      label = VALUES(label),
                      node_payload = VALUES(node_payload),
                      observed_at = GREATEST(COALESCE(observed_at, VALUES(observed_at)), COALESCE(VALUES(observed_at), observed_at)),
                      updated_at = CURRENT_TIMESTAMP(6)""".update.run
      apNodes <- sql"""INSERT INTO atheros_search.graph_nodes (
                        node_id, node_kind, label, node_payload, location_id, sensor_id,
                        normalized_mac, normalized_ssid, is_threat, observed_at, projection_run_id
                      )
                      SELECT CONCAT('ap:', frame.bssid), 'access_point', MAX(frame.ssid),
                             JSON_OBJECT('bssid', frame.bssid), MAX(frame.location_id), MAX(frame.sensor_id),
                             frame.bssid, MAX(frame.ssid), 0, MAX(frame.observed_at), $projectionRunId
                      FROM wireless_frames frame
                      WHERE frame.bssid IS NOT NULL
                      GROUP BY frame.bssid
                      ORDER BY MAX(frame.observed_at) DESC, frame.bssid
                      LIMIT $batchLimit
                      ON DUPLICATE KEY UPDATE
                        label = COALESCE(VALUES(label), label),
                        node_payload = VALUES(node_payload),
                        location_id = COALESCE(VALUES(location_id), location_id),
                        sensor_id = COALESCE(VALUES(sensor_id), sensor_id),
                        observed_at = GREATEST(COALESCE(observed_at, VALUES(observed_at)), COALESCE(VALUES(observed_at), observed_at)),
                        updated_at = CURRENT_TIMESTAMP(6)""".update.run
      clusterNodes <- sql"""INSERT INTO atheros_search.graph_nodes (
                             node_id, node_kind, label, node_payload,
                             is_threat, observed_at, projection_run_id
                           )
                           SELECT CONCAT('identity:', cluster.cluster_id), 'identity_cluster',
                                  cluster.cluster_name,
                                  JSON_OBJECT('cluster_size', cluster.cluster_size),
                                  0, cluster.last_seen, $projectionRunId
                           FROM atheros_search.identity_clusters cluster
                           WHERE cluster.status = 'active'
                           ORDER BY cluster.last_seen DESC, cluster.cluster_id
                           LIMIT $batchLimit
                    ON DUPLICATE KEY UPDATE
                      label = VALUES(label),
                      node_payload = VALUES(node_payload),
                      observed_at = GREATEST(COALESCE(observed_at, VALUES(observed_at)), COALESCE(VALUES(observed_at), observed_at)),
                      updated_at = CURRENT_TIMESTAMP(6)""".update.run
      edges <- sql"""INSERT INTO atheros_search.graph_edges (
                      edge_id, source_node_id, target_node_id, edge_kind,
                      weight, label, evidence, observed_at, projection_run_id
                    )
                    SELECT CONCAT('observed:', frame.source_mac, ':', frame.bssid),
                           CONCAT('device:', frame.source_mac), CONCAT('ap:', frame.bssid),
                           'observed_at', COUNT(*), 'wireless observation',
                           JSON_OBJECT('frame_count', COUNT(*)), MAX(frame.observed_at), $projectionRunId
                    FROM wireless_frames frame
                    WHERE frame.source_mac IS NOT NULL AND frame.bssid IS NOT NULL
                    GROUP BY frame.source_mac, frame.bssid
                    ORDER BY MAX(frame.observed_at) DESC, frame.source_mac, frame.bssid
                    LIMIT $batchLimit
                    ON DUPLICATE KEY UPDATE
                      weight = VALUES(weight),
                      evidence = VALUES(evidence),
                      observed_at = GREATEST(COALESCE(observed_at, VALUES(observed_at)), COALESCE(VALUES(observed_at), observed_at)),
                      updated_at = CURRENT_TIMESTAMP(6)""".update.run
      identityEdges <- sql"""INSERT INTO atheros_search.graph_edges (
                              edge_id, source_node_id, target_node_id, edge_kind,
                              weight, label, evidence, observed_at, projection_run_id
                            )
                            SELECT CONCAT('identity-member:', member.cluster_id, ':', member.mac),
                                   CONCAT('device:', member.mac), CONCAT('identity:', member.cluster_id),
                                   'identity_member', member.confidence, 'approved identity membership',
                                   member.evidence, member.last_seen, $projectionRunId
                            FROM atheros_search.identity_cluster_members member
                            ORDER BY member.last_seen DESC, member.cluster_id, member.mac
                            LIMIT $batchLimit
                            ON DUPLICATE KEY UPDATE
                              weight = VALUES(weight),
                              evidence = VALUES(evidence),
                              observed_at = GREATEST(COALESCE(observed_at, VALUES(observed_at)), COALESCE(VALUES(observed_at), observed_at)),
                              updated_at = CURRENT_TIMESTAMP(6)""".update.run
    yield deviceNodes + apNodes + clusterNodes + edges + identityEdges
