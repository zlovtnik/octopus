package com.sslproxy.coordinator.persistence

import com.sslproxy.coordinator.domain.{BrokerRecordMetadata, IngestionDecision}
import com.sslproxy.coordinator.processor.{ProcessorId, ProcessorStatus}
import com.sslproxy.coordinator.tidb.{LeaseIdentity, OutboxFailureDisposition, OutboxRecord}

import java.time.Instant

trait IngestionStore[F[_]]:
  def pendingCount: DbResultT[F, Long]
  def ensureCursors(streamNames: List[String]): DbResultT[F, Unit]

trait OutboxStore[F[_]]:
  def claim(ownerId: String, destinations: List[String], limit: Int, leaseSeconds: Int): DbResultT[F, List[OutboxRecord]]
  def acknowledge(outboxId: String, lease: LeaseIdentity): DbResultT[F, Boolean]
  def fail(outboxId: String, lease: LeaseIdentity, error: String): DbResultT[F, OutboxFailureDisposition]
  def recoverExpired: DbResultT[F, Int]

trait ResultStore[F[_]]:
  def recordEvidence(metadata: BrokerRecordMetadata, dedupeKey: String): DbResultT[F, Unit]

trait WirelessStore[F[_]]:
  def recordScan(metadata: BrokerRecordMetadata): DbResultT[F, IngestionDecision]

trait ProjectionStore[F[_]]:
  def reconcile(processorId: ProcessorId, limit: Int): DbResultT[F, Int]

trait MaintenanceStore[F[_]]:
  def retain(policy: String, cutoff: Instant, limit: Int): DbResultT[F, Int]
  def cleanStaleWorkers(before: Instant, limit: Int): DbResultT[F, Int]

trait ProcessorStateStore[F[_]]:
  def load: DbResultT[F, Map[ProcessorId, ProcessorStatus]]
  def persist(id: ProcessorId, status: ProcessorStatus, observedAt: Instant): DbResultT[F, Unit]
