const C_MAESTRO_EVENT_PREFIX = 'com.symphony.s2.model.chat.MaestroMessage';

export default class Metrics {

    constructor() {
        this.metrics = {
            hops: {},
            internalLatency: {},
            staleFeeds: {}
        };
        this.metricsCreatedDate = Date.now();
    }

    getMetrics() {
        return this.metrics;
    }

    setMessageId(messageId) {
        this.messageId = messageId;
    }

    getMetricsCreatedDate() {
        return this.metricsCreatedDate;
    }

    setMetricsGeneratedBy(source) {
        this.metrics.messageGeneratedBy = source;
    }

    setPayloadType(payloadType) {
        try {
            let truncPayloadType = payloadType;
            if (truncPayloadType.startsWith(C_MAESTRO_EVENT_PREFIX)) {
                truncPayloadType = C_MAESTRO_EVENT_PREFIX;
            }
            this.metrics.payloadType = truncPayloadType;
        } catch (error) {
            // do nothing
        }
    }

    setInterArrivalTime(lastMessageReceivedTimestamp) {
        this.metrics.interArrivalTime = this.metricsCreatedDate - lastMessageReceivedTimestamp;
    }

    setDistributionListSize(distributionListSize) {
        this.metrics.distributionListSize = distributionListSize;
    }

    setIsMessageExpired(value) {
        this.metrics.isMessageExpired = value;
    }

    setIsValidMessage(value) {
        this.metrics.isValidMessage = value;
    }

    setSbeToS2FwdElapsedTime(latency) {
        this.metrics.hops.sbeToS2FwdElapsedTime = latency;
    }

    setS2FwdToSentToSqsElapsedTime(latency) {
        this.metrics.hops.s2FwdToSentToSqsElapsedTime = latency;
    }

    setSentToSqsToRcvByDf2FanoutElapsedTime(latency) {
        this.metrics.hops.sentToSqsToRcvByDf2FanoutElapsedTime = latency;
    }

    calculateDF2FanoutInternalProcessingTime() {
        this.metrics.hops.df2FanoutInternalProcessingTime = Date.now() - this.metricsCreatedDate;
    }

    calculateInternalLatencyForValidate(elapsedTime) {
        this.metrics.internalLatency.validate = elapsedTime;
    }

    calculateInternalLatencyForSplit(elapsedTime) {
        this.metrics.internalLatency.split = elapsedTime;
    }

    calculateInternalLatencyForFetchFeeds(elapsedTime) {
        this.metrics.internalLatency.fetchFeeds = elapsedTime;
    }

    calculateInternalLatencyForFanout(elapsedTime) {
        this.metrics.internalLatency.fanout = elapsedTime;
    }

    calculateInternalLatencyForPushBack(elapsedTime) {
        this.metrics.internalLatency.pushback = elapsedTime;
    }

    calculateInternalLatencyForDeleteStaleFeeds(elapsedTime) {
        this.metrics.internalLatency.deleteStaleFeeds = elapsedTime;
    }

    calculateInternalLatencyForFeedsRecycling(elapsedTime) {
        this.metrics.internalLatency.recyclingFeeds = elapsedTime;
    }

    setIsProcessedWithNoErrors(value) {
        this.metrics.isProcessedWithNoErrors = value;
    }

    setRetryProcessingNumber(value) {
        this.metrics.retryProcessingNumber = value;
    }

    setIsAllowedMessage(value) {
        this.metrics.isAllowedMessage = value;
    }

    setIsMessageSplit(value) {
        this.metrics.isMessageSplit = value;
    }

    setNumberOfFetchedFeeds(value) {
        this.metrics.numberOfFetchedFeeds = value;
    }

    setStaleFeeds(detectedStaleFeeds, deletedStaleFeeds) {
        this.metrics.staleFeeds.detected = detectedStaleFeeds;
        this.metrics.staleFeeds.deleted = deletedStaleFeeds;
    }

    setIsBroadcast(value) {
        this.metrics.isBroadcast = value;
    }

    setLatencyToGetPayloadFromS3(value) {
        if (!this.metrics.vlm) {
            this.metrics.vlm = {
                latencyToGetPayloadFromS3: value,
                cache: {}
            };
        } else {
            this.metrics.vlm.latencyToGetPayloadFromS3 = value;
        }
    }

    setCacheResults(cacheResults) {
        if (!this.metrics.vlm) {
            this.metrics.vlm = {
                cache: cacheResults
            };
        } else {
            this.metrics.vlm.cache = cacheResults;
        }
    }

}
