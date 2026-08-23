package com.sslproxy.coordinator.postgres.sql

import com.sslproxy.coordinator.processor.{ApRiskProjection, DnsThreatCandidate, DnsThreatProjection}
import doobie.{ConnectionIO, Query0}
import doobie.implicits.*

object ThreatRiskSql:
  def dnsCandidates(limit: Int): Query0[DnsThreatCandidate] =
    sql"""SELECT host,
                  COUNT(*),
                  COALESCE(SUM(bytes_up + bytes_down), 0),
                  SUM(CASE WHEN event_time >= (CURRENT_TIMESTAMP + (-1) * INTERVAL '1 day') THEN 1 ELSE 0 END),
                  MAX(event_time)
           FROM proxy_events
           WHERE blocked = true
             AND event_time >= (CURRENT_TIMESTAMP + (-7) * INTERVAL '1 day')
           GROUP BY host
           HAVING COUNT(*) > 1
           ORDER BY MAX(event_time) DESC, host
           LIMIT ${limit.max(1)}""".query[DnsThreatCandidate]

  def persistDnsThreat(value: DnsThreatProjection): ConnectionIO[Int] =
    sql"""INSERT INTO atheros_search.threat_signals (
             source_key, near_duplicate, shadow_open, risk_score, ap_risk,
             threat_tag_count, signal_id, signal_type, dedupe_key,
             score, severity, explanation_text, evidence, detected_at,
             projection_run_id, updated_at
           ) VALUES (
             ${value.sourceKey}, 0, 0, ${value.score}, 0, 1,
             ${value.signalId}, 'dns_blocked_host', ${value.signalId},
             ${value.score}, ${value.severity},
             ${s"blocked DNS/host activity exceeded the characterized seven-day score for ${value.sourceKey}"},
             CAST(${value.evidenceJson} AS JSON), ${value.detectedAt}, ${value.projectionRunId},
             CURRENT_TIMESTAMP
           ) ON CONFLICT (source_key) DO UPDATE SET
             risk_score = EXCLUDED.risk_score,
             score = EXCLUDED.score,
             severity = EXCLUDED.severity,
             explanation_text = EXCLUDED.explanation_text,
             evidence = EXCLUDED.evidence,
             detected_at = EXCLUDED.detected_at,
             projection_run_id = EXCLUDED.projection_run_id,
             updated_at = CURRENT_TIMESTAMP""".update.run

  def apRiskCandidates(limit: Int): Query0[(String, Double, Double, Double, Double, Double)] =
    sql"""WITH bssids AS (
             SELECT frame.bssid
             FROM wireless_frames frame
             WHERE frame.bssid IS NOT NULL
             GROUP BY frame.bssid
             ORDER BY MAX(frame.observed_at) DESC, frame.bssid
             LIMIT ${limit.max(1)}
           ),
           alert_scores AS (
             SELECT COALESCE(alert.primary_mac, alert.secondary_mac) AS bssid,
                    MAX(CASE WHEN alert.alert_type IN ('deauth_flood', 'rogue_cluster') THEN 1 ELSE 0 END) AS deauth_score,
                    MAX(CASE WHEN alert.alert_type IN ('signal_anomaly', 'rogue_cluster') THEN 1 ELSE 0 END) AS signal_score,
                    MAX(CASE WHEN alert.alert_type = 'rogue_cluster' THEN 1 ELSE 0 END) AS typosquat_score
             FROM wireless_alerts alert
             JOIN bssids ON bssids.bssid = COALESCE(alert.primary_mac, alert.secondary_mac)
             WHERE alert.detected_at >= (CURRENT_TIMESTAMP + (-1) * INTERVAL '1 hour')
             GROUP BY COALESCE(alert.primary_mac, alert.secondary_mac)
           ),
           vendor_counts AS (
             SELECT peer.ssid, COUNT(DISTINCT peer.bssid_oui) AS vendor_count
             FROM wireless_frames peer
             WHERE peer.ssid IS NOT NULL
               AND peer.observed_at >= (CURRENT_TIMESTAMP + (-1) * INTERVAL '1 hour')
             GROUP BY peer.ssid
           ),
           vendor_scores AS (
             SELECT frame.bssid,
                    GREATEST(MAX(vendor_counts.vendor_count) - 1, 0) AS vendor_score
             FROM wireless_frames frame
             JOIN bssids ON bssids.bssid = frame.bssid
             JOIN vendor_counts ON vendor_counts.ssid = frame.ssid
             WHERE frame.bssid IS NOT NULL AND frame.ssid IS NOT NULL
               AND frame.observed_at >= (CURRENT_TIMESTAMP + (-1) * INTERVAL '1 hour')
             GROUP BY frame.bssid
           ),
           attributed_outliers AS (
             SELECT left_document.bssid AS bssid, pair.cosine_distance
             FROM atheros_search.similarity_pairs pair
             JOIN atheros_search.search_documents left_document
               ON left_document.document_id = pair.left_document_id
             JOIN atheros_search.search_documents right_document
               ON right_document.document_id = pair.right_document_id
             WHERE pair.computed_at >= (CURRENT_TIMESTAMP + (-1) * INTERVAL '1 hour')
               AND pair.cosine_distance > 0.15
             UNION ALL
             SELECT right_document.bssid AS bssid, pair.cosine_distance
             FROM atheros_search.similarity_pairs pair
             JOIN atheros_search.search_documents left_document
               ON left_document.document_id = pair.left_document_id
             JOIN atheros_search.search_documents right_document
               ON right_document.document_id = pair.right_document_id
             WHERE pair.computed_at >= (CURRENT_TIMESTAMP + (-1) * INTERVAL '1 hour')
               AND pair.cosine_distance > 0.15
           ),
           outlier_scores AS (
             SELECT attributed.bssid, MAX(attributed.cosine_distance) AS outlier_score
             FROM attributed_outliers attributed
             JOIN bssids ON bssids.bssid = attributed.bssid
             GROUP BY attributed.bssid
           )
           SELECT bssids.bssid,
                  COALESCE(alert_scores.deauth_score, 0),
                  COALESCE(alert_scores.signal_score, 0),
                  COALESCE(alert_scores.typosquat_score, 0),
                  COALESCE(vendor_scores.vendor_score, 0),
                  COALESCE(outlier_scores.outlier_score, 0)
           FROM bssids
           LEFT JOIN alert_scores ON alert_scores.bssid = bssids.bssid
           LEFT JOIN vendor_scores ON vendor_scores.bssid = bssids.bssid
           LEFT JOIN outlier_scores ON outlier_scores.bssid = bssids.bssid
           ORDER BY bssids.bssid""".query[(String, Double, Double, Double, Double, Double)]

  def persistApRisk(value: ApRiskProjection): ConnectionIO[Int] =
    sql"""INSERT INTO atheros_search.ap_risk_scores (
             bssid, composite_risk, signal_risk, identity_risk, behaviour_risk,
             evidence, measured_at, projection_run_id, updated_at
           ) VALUES (
             ${value.bssid}, ${value.composite}, ${value.signalRisk},
             ${value.identityRisk}, ${value.behaviorRisk},
             CAST(${value.evidenceJson} AS JSON), CURRENT_TIMESTAMP, ${value.projectionRunId},
             CURRENT_TIMESTAMP
           ) ON CONFLICT (bssid) DO UPDATE SET
             composite_risk = EXCLUDED.composite_risk,
             signal_risk = EXCLUDED.signal_risk,
             identity_risk = EXCLUDED.identity_risk,
             behaviour_risk = EXCLUDED.behaviour_risk,
             evidence = EXCLUDED.evidence,
             measured_at = EXCLUDED.measured_at,
             projection_run_id = EXCLUDED.projection_run_id,
             updated_at = CURRENT_TIMESTAMP""".update.run
