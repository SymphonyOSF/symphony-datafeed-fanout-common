import _ from 'lodash';
import { DF2Constants, generateVLMCacheKeyPrefix, generateCacheKeysToReplicate } from 'symphony-datafeed-core';

import validatePayloadType from './checkers/PayloadValidator';
import PipelineError from './exceptions/PipelineError';
import Telemetry from './telemetry/Telemetry';
import Metrics from './telemetry/Metrics';
import MetricsConstants from './telemetry/MetricsConstants';

class DF2FanoutHandler {

    constructor({
        databaseService, busService, objectStorageService, feedService, cacheRepoService, logger, options, hostname
    }) {
        this.databaseService = databaseService;
        this.busService = busService;
        this.objectStorageService = objectStorageService;
        this.feedService = feedService;
        this.cacheRepoService = cacheRepoService;

        this.environment = _.get(options, 'environment');
        this.startSplitFromRoomSize = _.get(options, 'fanout.splitting.startSplitFromRoomSize');
        this.dlSizeOfSplitMsg = _.get(options, 'fanout.splitting.distributionListSizeOfSplitMsg');
        this.cutoffLimitPubToSqs = _.get(options, 'fanout.messaging.vlm.cutoffLimitPubToSqs');
        this.staleFeedsTtl = _.get(options, 'staleFeedsTtl');
        this.itemExpirationTimeInSec = _.get(options, 'cache.itemExpirationTimeInSec');

        this.cacheType = _.get(options, 'cache.type');

        this.lastMessageProcessedTimestamp = 0;
        this.hostname = hostname;

        this.isMetricsExportationActivated = _.get(options, 'telemetry.isExportationActivated');
        this.logger = logger;
    }

    calculateMessageSize(message) {
        const messageInStringFormat = JSON.stringify(message);
        return messageInStringFormat.length;
    }

    async parseMessage(message) {
        try {
            const { isRawData } = message;

            // the message can come with the payload in json or string, let's check if we have to parse

            if (isRawData) {
                message.data = JSON.parse(message.data);
            }

            this.logger.debug('id[%s] - STEP::Message parsing. Received message = %o', message.id, message);

            /**
             * If the message has the field originalMessageId, it means that this message was
             * already processed by the function, and was re-inserted in the queue because it was
             * split in the previous processing, let's check and assign the original id to the
             * current id, this way the message tracing is easier to track in the logs
             */

            if (message.data.originalMessageId) {
                this.logger.debug('id[%s] - STEP::Message parsing. Re-inserted message, using original message id = %s', message.id, message.data.originalMessageId);
                message.id = message.data.originalMessageId;
            }

            if (message.data.isSplit || (message.data.retries && message.data.retries > 0)) {
                /**
                 * If a message is split, it means df2 fanout already processed this message and
                 * re-inserted the message in the ingestion queue, so the message is ready to go
                 * to the rest of the main flow, i.e., it was already processed in all validation/
                 * check steps in the main flow
                 * or this is a retry to process the message. If message has a retry, it is because
                 * the message got some errors in previous processing, like a socket timeout when
                 * trying to fanout the message to the destinations
                 */

                return { isVLM: false, isReInsertedMessage: true };
            }

            // it is a message received from the very first time from the pod

            if (message.data.payload) {
                message.data.payload = JSON.parse(Buffer.from(message.data.payload, 'base64').toString());

                this.logger.debug('id[%s] - STEP::Message parsing. Parsed payload for message = %o', message.id, message);

                return { isVLM: false, isReInsertedMessage: false };
            }

            // message does not have payload, let's try to get it from S3

            const initTsToFetchFromS3 = Date.now();
            const payload = await this.objectStorageService.getPayload(message.data);
            const base64payload = Buffer.from(payload, 'base64').toString();
            message.data.payload = JSON.parse(base64payload);
            const finalTsToFetchFromS3 = Date.now();

            this.logger.debug('id[%s] - STEP::Message parsing. VLM - Latency to fetch from s3 [%d] ms - Message = %o', message.id, finalTsToFetchFromS3 - initTsToFetchFromS3, message);
            return { isVLM: true, latencyToGetPayloadFromS3: finalTsToFetchFromS3 - initTsToFetchFromS3, isReInsertedMessage: false };

        } catch (error) {
            this.logger.error('id[%s] - STEP::Message parsing - Error %o. Message = %o', message.id, error, message);
            throw new PipelineError({ message: `id[${message.id}] - STEP::Message parsing`, discardOriginalMessage: true });
        }
    }

    validateMessage(message) {
        this.logger.debug('id[%s] - STEP::Content validation', message.id);

        const payloadType = _.get(message, 'data.payload.payloadType');
        if (_.isUndefined(payloadType)) {
            this.logger.info('id[%s] - STEP::Content validation - missing payloadType. Message = %o', message.id, message);
            throw new PipelineError({ message: `id[${message.id}] -  STEP::Content validation - Missing payloadType`, discardOriginalMessage: true });
        }

        const podId = _.get(message, 'data.payload.podId');
        if (_.isUndefined(podId)) {
            this.logger.info('id[%s] - STEP::Content validation - missing podId. Message = %o', message.id, message);
            throw new PipelineError({ message: `id[${message.id}] -  STEP::Content validation - Missing podId`, discardOriginalMessage: true });
        }

        return { podId, payloadType };
    }

    isAllowedMessage(message, isReInsertedMessage, payloadType) {
        if (!isReInsertedMessage) {
            const validationResult = validatePayloadType(message.data.payload, payloadType);
            if (!validationResult.success) {
                this.logger.info('id[%s] - STEP::Content validation - Not allowed message[%s]: %o', message.id, validationResult.message, message.data);
                return false;
            }
        }

        return true;
    }

    isMessageExpired(message) {
        const expirationDate = _.get(message, 'data.payload.expirationDate');
        if (expirationDate) {
            const convertedExpirationDate = new Date(expirationDate).getTime();
            if (convertedExpirationDate <= Date.now()) {
                this.logger.debug('id[%s] - STEP::Content validation - message expired', message.id);
                return true;
            }
        }

        return false;
    }

    async handleVLM(message, distributionList) {
        this.logger.debug('id[%s] - STEP::VLM validation', message.id);

        const result = {
            isActivated: false,
            type: this.cacheType
        };

        const messageSize = this.calculateMessageSize(message.data);
        const fullDistributionListSize = this.calculateMessageSize(distributionList);
        const numberOfCommas = this.dlSizeOfSplitMsg > 0 ? this.dlSizeOfSplitMsg - 1 : 0;
        // eslint-disable-next-line no-magic-numbers
        const maxDLSizeOfSplitMsg = 2 /* 2 brackets */ + numberOfCommas + (DF2Constants.DynamoDB.FEED_PK_USER_PREFIX.length + 14 /* user id size */) * this.dlSizeOfSplitMsg; // brackets + commas + recipients

        const finalMessageSize = this.isMessageSplittable(distributionList) ? messageSize - fullDistributionListSize + maxDLSizeOfSplitMsg : messageSize;

        this.logger.debug('id[%s] - STEP::VLM validation: message size[%d] | full DL size[%d] | max DL size of split msg [%d] -> final msg size [%d]', message.id, messageSize, fullDistributionListSize, maxDLSizeOfSplitMsg, finalMessageSize);

        /**
         * Let's calculate the message size minus DL size because the DL will not be used in the
         * publish to SQS. If the message if fanouted to a feed, it goes without the DL, obviously,
         * and if the message is re-inserted in the queue, it goes with a small DL with max size is
         * equal to distributionListSizeOfSplitMsg.
         */

        if (finalMessageSize > this.cutoffLimitPubToSqs) {
            /**
             * remove the internal payload and let it be retrieved in the
             * future by the feeder, because we are going to fanout to N-SQS
             * feed queues, we cannot write a message > 256kb as well in the
             * queue.
             *
             * Now we are going to write the payload in the cache in order to
             * be retrieved by the feeder very quickly and avoiding a burst
             * in S3, e.g., a VLM from a VLR where many clients are connected
             *
             * Feeder can retrieve the payload in the future because it will
             * receive the cache key and also the s3 properties as a fallback.
             */
            try {
                if (this.cacheRepoService) {

                    result.isActivated = true;

                    const keyPrefix = generateVLMCacheKeyPrefix(message.data);
                    const keys = generateCacheKeysToReplicate(keyPrefix);
                    const promises = [];
                    this.logger.debug('Received a VLM and storing its payload at key [%s]', keyPrefix);
                    const internalPayload = _.get(message, 'data.payload.payload');
                    if (!_.isEmpty(internalPayload)) {
                        const d1cache = Date.now();
                        const internalPayloadString = JSON.stringify(internalPayload);
                        for (let i = 0; i < keys.length; i++) {
                            promises.push(this.cacheRepoService.set(keys[ i ], internalPayloadString, this.itemExpirationTimeInSec));
                        }
                        const results = await Promise.allSettled(promises);
                        const d2cache = Date.now();

                        result.latencyToSetPayloadIntoCache = d2cache - d1cache;

                        if (results.some(promiseResult => promiseResult.status === 'rejected')) {
                            let errors = '';
                            results.forEach(promiseResult => {
                                if (promiseResult.status === 'rejected') {
                                    errors = errors + JSON.stringify(promiseResult.reason) + '\n';
                                }
                            });
                            if (errors.length > 0) {
                                throw new Error(errors);
                            }
                        }
                        this.logger.debug('id[%s] Finish replication for key[%s] - result %o', message.id, keyPrefix, results);
                    } else {
                        this.logger.info('id[%s] Cannot cache VLM: Empty payload', message.id);
                    }
                } else {
                    this.logger.debug('id[%s] Cannot cache VLM: Redis not available', message.id);
                }
            } catch (redisError) {
                this.logger.warn('id[%s] Cannot cache VLM: Redis error: %o', message.id, redisError);
            }

            /**
             *  Removing the internal payload
             */
            message.data.payload.payload = null;
        } else {
            this.logger.debug('id[%s] - STEP::VLM validation - payload can be sent within the message', message.id);
        }

        return result;
    }

    adaptDistributionList(id, podId, distributionList) {
        this.logger.debug('id[%s] - STEP::adaptDistributionList', id);

        for (let i = 0; i < distributionList.length; i++) {
            distributionList[ i ] = `${DF2Constants.DynamoDB.FEED_PK_USER_PREFIX}${distributionList[ i ]}`;
        }

        // pod id is added at the end of the list in order to fetch datahose feeds

        distributionList.push(`${DF2Constants.DynamoDB.FEED_PK_POD_PREFIX}${podId}`);
    }

    isMessageSplittable(distributionList) {
        return distributionList && distributionList.length >= this.startSplitFromRoomSize;
    }

    checkSplit(id, messageData, distributionList) {
        const isSplittable = this.isMessageSplittable(distributionList);
        if (isSplittable) {
            const copies = _.chunk(distributionList, this.dlSizeOfSplitMsg);
            if (copies.length > 1) {
                return {
                    split: true,
                    splits: copies.map(chunkDL => {
                        return {
                            // add the original message id here to keep message tracking in the logs
                            originalMessageId: id,
                            ...messageData,
                            isSplit: true,
                            payload: {
                                ...messageData.payload,
                                distributionList: chunkDL
                            }
                        };
                    })
                };
            }
        }
        return { split: false };
    }

    async mustSplitMessage(message, podId, distributionList) {
        try {
            const { split, splits } = this.checkSplit(message.id, message.data, distributionList);
            if (split) {
                this.logger.debug('id[%s] - STEP::DO-Split', message.id);
                // first message in the split array will contain the large DL
                const messageSize = this.calculateMessageSize(splits[ 0 ]);
                await this.busService.handleSplit(splits, messageSize);
                return true;
            }

            this.logger.debug('id[%s] - STEP::NO-Split', message.id);
            return false;

        } catch (error) {
            const msg = `STEP::Split: ${error.message}, podId: ${podId}`;
            this.logger.warn(msg);
            throw new PipelineError({ message: msg, discardOriginalMessage: false });
        }
    }

    async sendBroadcastMessage(message, podId) {
        this.logger.debug('id[%s] - STEP::broadcast', message.id);

        try {
            const isBroadcast = _.get(message, 'data.payload.broadcast') === 'ALL';
            if (isBroadcast) {
                this.logger.info('type[audit] broadcast messageId[%s]', _.get(message.data, 'data.payload.payload.messageId'));
                await this.busService.broadcastMessage(message.data, podId);
            }
            return isBroadcast;
        } catch (error) {
            const msg = `STEP::Broadcast: ${error.message}, podId: ${podId}`;
            if (/topic does not exist/i.test(error.message)) {
                this.logger.warn(`${msg} - CHECK BROADCAST SUBSCRIPTION FLOW!`);
                throw new PipelineError({ message: msg, discardOriginalMessage: true });
            } else {
                this.logger.warn(msg);
                throw new PipelineError({ message: msg, discardOriginalMessage: false });
            }
        }
    }

    adaptMessage(message, isFromEcs) {
        const { messageId, body, attributes } = message;
        if ((!messageId || !body) && !isFromEcs) {
            throw new PipelineError({ message: 'Missing params in the message', discardOriginalMessage: true });
        }

        try {
            let data = {};
            let isRawData = false;
            const parsedBody = isFromEcs ? message : JSON.parse(body);

            if (parsedBody.Message) {
                isRawData = true;
                data = parsedBody.Message;
            } else {
                data = parsedBody;
            }

            return {
                isRawData,
                data,
                id: isFromEcs ? message.MessageId : messageId,
                attributes,
                timestamp: Date.now()
            };
        } catch (e) {
            return {
                data: undefined,
                id: messageId,
                attributes,
                timestamp: Date.now()
            };
        }
    }

    calculateExternalHops(message, telemetry, metrics) {
        const createdDate = Date.parse(_.get(message, 'data.payload.createdDate'));
        const notificationDate = Date.parse(_.get(message, 'data.payload.notificationDate'));
        const sentToSQS = parseInt(_.get(message, 'attributes.SentTimestamp'), 10);
        const rcvByTheService = telemetry.getTelemetryCreatedDate();
        const isValidMsgForwarderMessage = createdDate > 0 && notificationDate;

        if (isValidMsgForwarderMessage) {
            metrics.setSbeToS2FwdElapsedTime(notificationDate - createdDate);
        }

        if (notificationDate && sentToSQS) {
            metrics.setS2FwdToSentToSqsElapsedTime(sentToSQS - notificationDate);
        }

        if (sentToSQS && rcvByTheService) {
            metrics.setSentToSqsToRcvByDf2FanoutElapsedTime(rcvByTheService - sentToSQS);
        }
    }

    isNeitherTypingOrPresence(payloadType) {
        return payloadType !== DF2Constants.EventType.C_PRESENCE_TYPE
                && payloadType !== DF2Constants.EventType.C_TYPING_NOTIFICATION_TYPE;
    }

    async processRecord(record, telemetry, isProcessedByEcs = false) {
        /**
         * metrics will contain all measures computed for the current record under processing
         */
        const metrics = new Metrics();

        /**
         * we are not able to calculate the IAT only for the very first message received since
         * the lambda function has started
         */
        if (this.lastMessageProcessedTimestamp) {
            metrics.setInterArrivalTime(this.lastMessageProcessedTimestamp);
        }
        this.lastMessageProcessedTimestamp = metrics.getMetricsCreatedDate();
        const message = this.adaptMessage(record, isProcessedByEcs);

        try {
            this.logger.debug('id[%s] - Main Pipeline - START - message: %o', message.id, message);

            const { isVLM, isReInsertedMessage, latencyToGetPayloadFromS3 } = await this.parseMessage(message);

            metrics.setMessageId(message.id);

            if (!isReInsertedMessage) {
                /**
                 * receiving this message for the very first time
                 */
                metrics.setMetricsGeneratedBy(MetricsConstants.METRICS_GENERATED_BY_SBE);
            } else {
                /**
                 * it is a message already processed by the df2-fanout and re-added in the queue
                 */
                metrics.setMetricsGeneratedBy(MetricsConstants.METRICS_GENERATED_BY_DF2_FANOUT);
            }

            this.calculateExternalHops(message, telemetry, metrics);

            metrics.setRetryProcessingNumber(message.data.retries || 0);

            const validateD0 = Date.now();
            const { podId, payloadType } = this.validateMessage(message);
            metrics.calculateInternalLatencyForValidate(Date.now() - validateD0);

            if (this.isNeitherTypingOrPresence(payloadType)) {
                this.logger.info('type[audit] received messageId[%s] from MF', _.get(message, 'data.payload.payload.messageId'));
            }

            telemetry.setPodId(podId);

            metrics.setPayloadType(payloadType);
            metrics.setIsMessageExpired(false);
            metrics.setIsValidMessage(true);

            const isMessageExpired = this.isMessageExpired(message);

            metrics.setIsMessageExpired(isMessageExpired);

            if (!isMessageExpired) {

                const isAllowedMessage = this.isAllowedMessage(message, isReInsertedMessage, payloadType);

                metrics.setIsAllowedMessage(isAllowedMessage);

                if (isAllowedMessage) {

                    const distributionList = _.get(message, 'data.payload.distributionList', []);

                    if (isVLM) {
                        metrics.setLatencyToGetPayloadFromS3(latencyToGetPayloadFromS3);
                        const handleVlmResult = await this.handleVLM(message, distributionList);
                        metrics.setCacheResults(handleVlmResult);
                    }

                    const isBroadcast = await this.sendBroadcastMessage(message, podId);

                    metrics.setIsBroadcast(isBroadcast);

                    if (!isBroadcast) {
                        let mustSplitMsg = false;

                        metrics.setDistributionListSize(distributionList.length);

                        if (!isReInsertedMessage) {
                            this.adaptDistributionList(message.id, podId, distributionList);

                            const splitD0 = Date.now();
                            mustSplitMsg = await this.mustSplitMessage(message, podId, distributionList);
                            metrics.calculateInternalLatencyForSplit(Date.now() - splitD0);
                        }

                        metrics.setIsMessageSplit(mustSplitMsg);

                        if (!mustSplitMsg) {
                            const d1Fetch = Date.now();
                            this.logger.debug('id[%s] - STEP::findFeeds', message.id);
                            const {
                                feeds,
                                feedsItemsToBeDeleted,
                                feedsToBeStale,
                                feedsToBeReuse
                            } = await this.databaseService.fetchFeeds(message.data, distributionList);
                            const d2Fetch = Date.now();
                            metrics.calculateInternalLatencyForFetchFeeds(d2Fetch - d1Fetch);
                            const numberOfFetchedFeeds = feeds.length;
                            metrics.setNumberOfFetchedFeeds(numberOfFetchedFeeds);

                            this.logger.debug('id[%s] - STEP::findFeeds - DONE - fetched feeds[%d] - latency[%d]ms: %o', message.id, numberOfFetchedFeeds, d2Fetch - d1Fetch, feeds);

                            const d1Fanout = Date.now();
                            this.logger.debug('id[%s] - STEP::fanout INIT', message.id);
                            const fanoutResults = await this.busService.fanoutMessage(message.data, feeds, podId);
                            fanoutResults.forEach(fanoutResult => {
                                const isRejected = fanoutResult.status === 'rejected';
                                if (isRejected) {
                                    const rejectReason = _.get(fanoutResult, 'reason.message', '');
                                    const isNonExistentQueueReason = rejectReason === DF2Constants.Errors.ERROR_NONEXISTENT_QUEUE;
                                    if (isNonExistentQueueReason) {
                                        this.logger.debug('id[%s] - STEP::Fanout: nonexistent queue, but fanout proceeds normally, podId: %d', message.id, podId);
                                    } else {
                                        this.logger.error('id[%s] - STEP::Fanout: Fail on fanout message: %s, podId: %d. Message = %o', message.id, rejectReason, podId, message);
                                        throw new PipelineError({ message: 'Fanout not processed for all feeds', discardOriginalMessage: false });
                                    }
                                }
                            });
                            if (this.isNeitherTypingOrPresence(payloadType)) {
                                this.logger.info('type[audit] fanout done for messageId[%s]', _.get(message, 'data.payload.payload.messageId'));
                            }
                            const d2Fanout = Date.now();
                            metrics.calculateInternalLatencyForFanout(d2Fanout - d1Fanout);
                            this.logger.debug('id[%s] - STEP::fanout - DONE - latency[%d]ms', message.id, d2Fanout - d1Fanout);

                            const hasFeedsToRecycle = !_.isEmpty(feedsItemsToBeDeleted) || !_.isEmpty(feedsToBeStale) || !_.isEmpty(feedsToBeReuse);
                            if (hasFeedsToRecycle) {
                                const df1Recycling = Date.now();
                                this.logger.debug('id[%s] - STEP:: recycling feeds INIT', message.id);
                                this.logger.debug('id[%s] - STEP:: recycling feeds - feedsToRemove [ %o ], feedsToStale [ %o ], feedsToReuse [ %o ]', message.id, feedsItemsToBeDeleted, feedsToBeStale, feedsToBeReuse);
                                await this.feedService.recyclingFeeds(feedsToBeStale, feedsToBeReuse, feedsItemsToBeDeleted, podId);
                                const df2Recycling = Date.now();
                                metrics.calculateInternalLatencyForFeedsRecycling(df2Recycling - df1Recycling);
                                this.logger.debug('id[%s] - STEP::recycling feeds - DONE - latency[%d]ms', message.id, df2Recycling - df1Recycling);
                            }

                            this.logger.debug('id[%s] - Main Pipeline - DONE', message.id);
                        }
                    }
                }
            }
        } catch (errorInMainPipeline) {
            if (!errorInMainPipeline.discardOriginalMessage) {
                try {
                    const currentFailCounter = _.get(message, 'data.retries', 0);
                    message.data.retries = currentFailCounter + 1;
                    if (!message.data.originalMessageId) {
                        // add the original message id here to keep message tracking in the logs
                        message.data.originalMessageId = message.id;
                    }

                    const d1PushBack = Date.now();
                    const pushBackResult = await this.busService.pushBackMessage(message.data);
                    const d2PushBack = Date.now();
                    metrics.calculateInternalLatencyForPushBack(d2PushBack - d1PushBack);
                    this.logger.error('id[%s] - Main Pipeline: nack original message. - latency[%d]ms Result: %o - Original error: %s %j. Message = %o', message.id, d2PushBack - d1PushBack, pushBackResult, errorInMainPipeline.message, errorInMainPipeline, message);
                } catch (pushBackError) {

                    metrics.calculateDF2FanoutInternalProcessingTime();
                    metrics.setIsProcessedWithNoErrors(false);

                    telemetry.pushMetricsToTelemetry(metrics.getMetrics());

                    return Promise.reject(pushBackError);
                }
            } else {
                this.logger.error('id[%s] - Main Pipeline: discarding original message. Error: %s. Message = %o', message.id, errorInMainPipeline.message, message);
            }
        }

        metrics.calculateDF2FanoutInternalProcessingTime();
        metrics.setIsProcessedWithNoErrors(true);

        telemetry.pushMetricsToTelemetry(metrics.getMetrics());

        return Promise.resolve({ messageId: message.id, isProcessed: true });
    }

    async closeTelemetry(telemetry) {
        /**
         * adding the total elapsed time to the telemetry headers metrics
         */
        telemetry.computeTelemetryTotalElapsedTime();
        this.logger.debug('Telemetry: %o', telemetry);
        if (this.isMetricsExportationActivated) {
            try {
                await this.busService.sendTelemetry(telemetry.getTelemetry());
            } catch (errorExportingTelemetry) {
                this.logger.error('Error exporting telemetry: %o. Error: %s', telemetry, errorExportingTelemetry.message);
            }
        }
    }

    async consume(records) {
        const telemetry = new Telemetry(this.environment, this.hostname);
        const batchSize = records.length;
        const promises = [];

        /**
         * adding the batchSize to the telemetry headers metrics
         */
        telemetry.setBatchSize(batchSize);

        this.logger.debug('Received batch size: %d', batchSize);
        for (let i = 0; i < records.length; i++) {
            promises.push(this.processRecord(records[ i ], telemetry));
        }
        try {
            const recordsResult = await Promise.all(promises);
            return recordsResult;
        } finally {
            await this.closeTelemetry(telemetry);
        }
    }

    async consumeEcs(message) {
        const telemetry = new Telemetry(this.environment, this.hostname);
        try {
            await this.processRecord(message, telemetry, true);
            return 'fanout';
        } finally {
            await this.closeTelemetry(telemetry);
        }
    }

}

module.exports = DF2FanoutHandler;
