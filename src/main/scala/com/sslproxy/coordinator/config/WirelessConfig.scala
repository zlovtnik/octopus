package com.sslproxy.coordinator.config

import pureconfig.ConfigReader

final case class WirelessConfig(
    backlogSaveTopic: String,
    backlogSaveConsumer: String,
    backlogListTopic: String,
    backlogListConsumer: String,
    backlogListReplyTopic: String,
    backlogSyncedTopic: String,
    backlogSyncedConsumer: String,
    backlogPruneTopic: String,
    backlogPruneConsumer: String,
    backlogPruneReplyTopic: String,
    macLookupTopic: String,
    macLookupConsumer: String,
    macLookupReplyTopic: String,
    networksAuthorizedTopic: String,
    networksAuthorizedConsumer: String,
    networksAuthorizedReplyTopic: String,
    probeFlushTopic: String,
    probeFlushConsumer: String,
    consumersCount: Int,
    maxPollRecords: Int
) derives ConfigReader
