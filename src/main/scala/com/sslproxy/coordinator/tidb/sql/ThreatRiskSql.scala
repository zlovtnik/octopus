package com.sslproxy.coordinator.tidb.sql

import com.sslproxy.coordinator.processor.{ApRiskProjection, DnsThreatCandidate, DnsThreatProjection}
import doobie.{ConnectionIO, Query0}
import doobie.implicits.*

object ThreatRiskSql:
  def dnsCandidates(limit: Int): Query0[DnsThreatCandidate] =
    sql"""SELECT host,
                  COUNT(*),
                  COALESCE(SUM(bytes_up + bytes_down), 0),
                  SUM(CASE WHEN event_time >= TIMESTAMPADD(DAY, -1, CURRENT_TIMESTAMP(6)) THEN 1 ELSE 0 END),
                  MAX(event_time)
           FROM proxy_events
           WHERE blocked = 1
             AND event_time >= TIMESTAMPADD(DAY, -7, CURRENT_TIMESTAMP(6))
           GROUP BY host
           HAVING COUNT(*) > 1
           ORDER BY MAX(event_time) DESC, host
           LIMIT ${limit.max(1)}""".query[DnsThreatCandidate]

  def persistDnsThreat(value: DnsThreatProjection): ConnectionIO[Int] =
    sql"""INSERT INTO atheros_search.threat_signals (
             source_key, near_duplicate, shadow_open, risk_score, ap_risk,
             threat_tag_count, signal_id, signal_type, dedupe_key,
             score, severity, explanation_text, evidence, detected_at,
             projection_run_id
           ) VALUES (
             ${value.sourceKey}, 0, 0, ${value.score}, 0, 1,
             ${value.signalId}, 'dns_blocked_host', ${value.signalId},
             ${value.score}, ${value.severity},
             ${s"blocked DNS/host activity exceeded the characterized seven-day score for ${value.sourceKey}"},
             CAST(${value.evidenceJson} AS JSON), ${value.detectedAt}, ${value.projectionRunId}
           ) ON DUPLICATE KEY UPDATE
             risk_score = VALUES(risk_score),
             score = VALUES(score),
             severity = VALUES(severity),
             explanation_text = VALUES(explanation_text),
             evidence = VALUES(evidence),
             detected_at = VALUES(detected_at),
             projection_run_id = VALUES(projection_run_id),
             updated_at = CURRENT_TIMESTAMP(6)""".update.run

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
             WHERE alert.detected_at >= TIMESTAMPADD(HOUR, -1, CURRENT_TIMESTAMP(6))
             GROUP BY COALESCE(alert.primary_mac, alert.secondary_mac)
           ),
           vendor_scores AS (
             SELECT frame.bssid,
                    GREATEST(COUNT(DISTINCT peer.bssid_oui) - 1, 0) AS vendor_score
             FROM wireless_frames frame
             JOIN wireless_frames peer ON peer.ssid = frame.ssid
             WHERE frame.bssid IS NOT NULL AND frame.ssid IS NOT NULL
             GROUP BY frame.bssid
           ),
           outlier_scores AS (
             SELECT document.bssid, MAX(pair.cosine_distance) AS outlier_score
             FROM atheros_search.similarity_pairs pair
             JOIN atheros_search.search_documents document
               ON document.document_id IN (pair.left_document_id, pair.right_document_id)
             WHERE pair.computed_at >= TIMESTAMPADD(HOUR, -1, CURRENT_TIMESTAMP(6))
               AND pair.cosine_distance > 0.15
               AND document.bssid IS NOT NULL
             GROUP BY document.bssid
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
             evidence, measured_at, projection_run_id
           ) VALUES (
             ${value.bssid}, ${value.composite}, ${value.signalRisk},
             ${value.identityRisk}, ${value.behaviorRisk},
             CAST(${value.evidenceJson} AS JSON), CURRENT_TIMESTAMP(6), ${value.projectionRunId}
           ) ON DUPLICATE KEY UPDATE
             composite_risk = VALUES(composite_risk),
             signal_risk = VALUES(signal_risk),
             identity_risk = VALUES(identity_risk),
             behaviour_risk = VALUES(behaviour_risk),
             evidence = VALUES(evidence),
             measured_at = VALUES(measured_at),
             projection_run_id = VALUES(projection_run_id)""".update.run
