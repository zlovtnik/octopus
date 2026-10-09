package com.sslproxy.coordinator.wiring.workloads

import cats.effect.IO
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.cron.CronScheduler
import com.sslproxy.coordinator.processor.{ProcessorId, ProcessorWorkload, SearchRetentionProcessor}
import fs2.Stream

import scala.concurrent.duration.*

private[coordinator] object ScheduledWorkloads:
  def build(
      cfg: AppConfig,
      cronScheduler: CronScheduler,
      searchRetentionProcessor: SearchRetentionProcessor[IO],
      maintenanceInterval: FiniteDuration
  ): List[ProcessorWorkload] =
    List(
      ProcessorWorkload(
        ProcessorId.SyncJobPlanner,
        cronScheduler.jobPlanningStream
      ),
      ProcessorWorkload(
        ProcessorId.SyncBacklogRecovery,
        cronScheduler.backlogRecoveryStream
      ),
      ProcessorWorkload(
        ProcessorId.SyncLoadDispatch,
        cronScheduler.loadDispatchStream
      ),
      ProcessorWorkload(
        ProcessorId.SyncOutboxPublisher,
        cronScheduler.outboxPublisherStream
      ),
      ProcessorWorkload(
        ProcessorId.WirelessFrameNormalizer,
        cronScheduler.wirelessFrameNormalizerStream
      ),
      ProcessorWorkload(
        ProcessorId.WirelessInventoryProjector,
        cronScheduler.wirelessInventoryProjectorStream
      ),
      ProcessorWorkload(
        ProcessorId.WirelessIdentityProjector,
        cronScheduler.identityProjectorStream(
          cfg.processors.batchSize,
          cfg.processors.intervalSeconds.seconds,
          cfg.processors.behaviorSimilarityThreshold
        )
      ),
      ProcessorWorkload(
        ProcessorId.WirelessBehaviorProjector,
        cronScheduler.behaviorProjectorStream(
          cfg.processors.batchSize,
          cfg.processors.intervalSeconds.seconds
        )
      ),
      ProcessorWorkload(
        ProcessorId.WirelessTimingProjector,
        cronScheduler.timingProjectorStream(
          cfg.processors.batchSize,
          cfg.processors.intervalSeconds.seconds
        )
      ),
      ProcessorWorkload(
        ProcessorId.WirelessSequenceProjector,
        cronScheduler.sequenceProjectorStream(
          cfg.processors.batchSize,
          cfg.processors.intervalSeconds.seconds
        )
      ),
      ProcessorWorkload(
        ProcessorId.WirelessBaselineProjector,
        cronScheduler.baselineProjectorStream(
          cfg.processors.batchSize,
          cfg.processors.intervalSeconds.seconds
        )
      ),
      ProcessorWorkload(
        ProcessorId.WirelessSimilarityProjector,
        cronScheduler.similarityProjectorStream(
          cfg.processors.batchSize,
          cfg.processors.intervalSeconds.seconds,
          cfg.processors.eventDuplicateDistance,
          cfg.processors.behaviorSimilarityThreshold,
          cfg.processors.sequenceDistanceThreshold
        )
      ),
      ProcessorWorkload(
        ProcessorId.ThreatRiskProjector,
        cronScheduler.threatRiskProjectorStream(
          cfg.processors.batchSize,
          cfg.processors.intervalSeconds.seconds
        )
      ),
      ProcessorWorkload(
        ProcessorId.EmbeddingTextBuilder,
        cronScheduler.searchDocumentBuilderStream(
          cfg.processors.batchSize,
          cfg.processors.intervalSeconds.seconds
        )
      ),
      ProcessorWorkload(
        ProcessorId.EmbeddingPreparer,
        cronScheduler.embeddingJobPreparerStream(
          cfg.processors.batchSize,
          cfg.processors.intervalSeconds.seconds,
          cfg.processors.embeddingModel,
          cfg.processors.embeddingPendingHighWater
        )
      ),
      ProcessorWorkload(
        ProcessorId.SearchRetention,
        Stream
          .awakeEvery[IO](
            maintenanceInterval
          )
          .evalMap(_ =>
            searchRetentionProcessor.runOnce
          )
      ),
      ProcessorWorkload(
        ProcessorId.StaleWorkerCleanup,
        cronScheduler.staleWorkerCleanupStream(
          cfg.processors.batchSize,
          maintenanceInterval
        )
      ),
      ProcessorWorkload(
        ProcessorId.ScheduledReconciliation,
        cronScheduler
          .scheduledReconciliationStream(
            cfg.processors.batchSize,
            maintenanceInterval
          )
      ),
      ProcessorWorkload(
        ProcessorId.RfAlertProjector,
        cronScheduler.rfAlertStream
      )
    )
