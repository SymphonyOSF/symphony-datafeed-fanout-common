/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
import _get from 'lodash/get';
import _isEmpty from 'lodash/isEmpty';
import removeFeeds from 'symphony-datafeed-core/removeFeeds';
import updateDdbFields from 'symphony-datafeed-core/updateDdbFields';
import DF2Constants from 'symphony-datafeed-core/DF2Constants';

import getHashedValue from './utils/getHashedValue';

class FeedService {

    constructor({
        sqsClient, ddbDocumentClient, logger, options, busService
    }) {
        this.sqsClient = sqsClient;
        this.ddbDocumentClient = ddbDocumentClient;
        this.busService = busService;

        this.aws = _get(options, 'aws');
        this.feedQueuePrefix = _get(options, 'fanout.feedQueuePrefix');
        this.topicNamePrefix = _get(options, 'fanout.messaging.broadcast.topicNamePrefix');
        this.subscriptionQueueName = _get(options, 'fanout.messaging.broadcastSubscriptionQueue.name');
        this.tableName = _get(options, 'db.table');
        this.useQueueUrlBuilderFromAWS = _get(options, 'sqs.endpoint');
        this.logger = logger;
    }

    logRejectedRemoveFeeds(removeFeedsPromise, deletedFeedsItems) {
        removeFeedsPromise.then(removeFeedPromises => {
            const ddbPromises = removeFeedPromises[ 0 ];
            // 1st item hold all ddb promises in a array. See core/removeFeeds.js
            for (let index = 0; index < ddbPromises.length; index++) {
                ddbPromises[ index ].then(ddbPromise => {
                    if (_get(ddbPromise, 'status') === 'rejected') {
                        this.logger.warn('Error to remove the feed %s from ddb reason: %s', deletedFeedsItems[ index ].feedId, ddbPromise.reason);
                    }
                });
            }
            // remaining items are sqs promise results
            for (let index = 1; index < removeFeedPromises.length; index++) {
                removeFeedPromises[ index ].then(promise => {
                    if (_get(promise, 'status') === 'rejected') {
                        this.logger.warn('Error to remove the sqs feed %s queue reason: %s', deletedFeedsItems[ index ].feedId, promise.reason);
                    }
                });
            }
        });
    }

    removeFeeds(deletedFeedsItems, podId) {
        const removeFeedsPromise = removeFeeds(
            this.sqsClient,
            this.ddbDocumentClient,
            deletedFeedsItems,
            {
                aws: this.aws,
                feedQueuePrefix: this.feedQueuePrefix,
                broadcast: {
                    topicNamePrefix: this.topicNamePrefix,
                    subscriptionQueueName: this.subscriptionQueueName
                },
                tableName: this.tableName,
                endpoint: this.useQueueUrlBuilderFromAWS
            },
            podId
        );
        this.logRejectedRemoveFeeds(removeFeedsPromise, deletedFeedsItems);

        return removeFeedsPromise;
    }

    async updateFeedsToStale(feeds = [], podId = null) {
        if (_isEmpty(feeds)) {
            return Promise.resolve();
        }

        let numFeedUpdatedToStale = 0;
        for (const feed of feeds) {
            const { feedId, userId, feedsKey } = feed;
            const updateObject = {
                [ `feeds.${feedId}.assignId` ]: null,
                [ `feeds.${feedId}.startDrainTs` ]: -1
            };
            const additionalConditions = {
                expressions:
                    [
                        `#${getHashedValue('feeds')}.#${getHashedValue(feedId)}.#${getHashedValue('assignId')}<> :minus_one`,
                        `not attribute_type(#${getHashedValue('feeds')}.#${getHashedValue(feedId)}.#${getHashedValue('assignId')}, :null_type)`
                    ],
                attributeValues: {
                    ':minus_one': -1,
                    ':null_type': 'NULL'
                }
            };
            const key = {
                [ DF2Constants.DynamoDB.FEEDS_TABLE_PK_NAME ]: feedsKey
            };
            try {
                await updateDdbFields(this.ddbDocumentClient, this.tableName, key, updateObject, additionalConditions);
                numFeedUpdatedToStale++;
            } catch (error) {
                this.logger.warn('Error to update to stale the feed %s of userId %d from pod %d: %o ', feedId, userId, podId, error);
            }
        }
        this.logger.debug('FeedsUpdated to stale = %d', numFeedUpdatedToStale);
        return { numFeedUpdatedToStale };
    }

    async updateFeedsToReuse(feeds = [], podId = null) {
        if (_isEmpty(feeds)) {
            return Promise.resolve();
        }
        let numFeedUpdatedToReuse = 0;
        for (const feed of feeds) {
            const { feedId, userId } = feed;
            try {
                await this.busService.sendRecycleFeed({
                    feedId,
                    userId,
                    podId,
                    action: 'drain',
                    origin: 'df2-fanout'
                });
                numFeedUpdatedToReuse++;
            } catch (error) {
                this.logger.warn(`Error to update to reuse the feed ${feedId} of userId ${userId} from pod ${podId}: ${error.message}`);
            }
        }
        this.logger.debug('FeedsUpdated to reuse = %d', numFeedUpdatedToReuse);
        return { numFeedUpdatedToReuse };
    }

    recyclingFeeds(feedsToStale = [], feedsToReuse = [], feedsToRemove = [], podId = null) {
        const promises = [];
        promises.push(this.updateFeedsToStale(feedsToStale, podId));
        promises.push(this.updateFeedsToReuse(feedsToReuse, podId));
        promises.push(this.removeFeeds(feedsToRemove, podId));

        return Promise.allSettled(promises);
    }

}

module.exports = FeedService;
