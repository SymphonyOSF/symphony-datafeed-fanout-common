/* eslint-disable consistent-return */
/* eslint-disable no-restricted-syntax */
/* eslint-disable no-await-in-loop */
import _ from 'lodash';
import { markFeedStale, removeFeeds } from 'symphony-datafeed-core';

class FeedService {

    constructor({
        sqsClient, ddbDaxClient, ddbDirectClient, logger, options, busService
    }) {
        this.sqsClient = sqsClient;
        this.ddbDaxClient = ddbDaxClient;
        this.ddbDirectClient = ddbDirectClient;
        this.busService = busService;

        this.aws = _.get(options, 'aws');
        this.feedQueuePrefix = _.get(options, 'fanout.feedQueuePrefix');
        this.topicNamePrefix = _.get(options, 'fanout.messaging.broadcast.topicNamePrefix');
        this.subscriptionQueueName = _.get(options, 'fanout.messaging.broadcastSubscriptionQueue.name');
        this.tableName = _.get(options, 'db.table');
        this.useQueueUrlBuilderFromAWS = _.get(options, 'sqs.endpoint');
        this.logger = logger;
    }

    logRejectedRemoveFeeds(removeFeedsPromises, deletedFeedsItems) {
        const ddbPromises = removeFeedsPromises[ 0 ];
        // 1st item hold all ddb promises in a array. See core/removeFeeds.js and core/ddb/removeFeeds.js
        for (let index = 0; index < ddbPromises.length; index++) {
            if (_.get(ddbPromises[ index ], 'status') === 'rejected') {
                this.logger.warn('Error to remove the feed %s from ddb reason: %s',
                    deletedFeedsItems[ index ].feedId, ddbPromises[ index ].reason);
            }
        }
        // remaining items are sqs promise results
        for (let index = 1; index < removeFeedsPromises.length; index++) {
            if (_.get(removeFeedsPromises[ index ], 'status') === 'rejected') {
                this.logger.warn('Error to remove the sqs feed %s queue reason: %s',
                    deletedFeedsItems[ index - 1 ].feedId, removeFeedsPromises[ index ].reason);
            }
        }
    }

    async removeFeeds(deletedFeedsItems, podId) {
        const result = await removeFeeds({
            sqsClient: this.sqsClient,
            ddbDaxClient: this.ddbDaxClient,
            ddbDirectClient: this.ddbDirectClient,
            feeds: deletedFeedsItems,
            options: {
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
        });
        this.logRejectedRemoveFeeds(result, deletedFeedsItems);
        return result;
    }

    async updateFeedsToStale(feeds = [], podId = null) {
        const result = { numFeedUpdatedToStale: 0 };
        if (!_.isEmpty(feeds)) {
            for (const feed of feeds) {
                const { feedId, userId, feedsKey } = feed;
                try {
                    await markFeedStale({
                        daxClient: this.ddbDaxClient,
                        directClient: this.ddbDirectClient,
                        tableName: this.tableName,
                        feedsKey,
                        feedId
                    });
                    result.numFeedUpdatedToStale++;
                } catch (error) {
                    this.logger.warn('Error to update to stale the feed %s of userId %d from pod %d: %o ', feedId, userId, podId, error);
                }
            }
            this.logger.debug('FeedsUpdated to stale = %d', result.numFeedUpdatedToStale);
        }
        return result;
    }

    async updateFeedsToReuse(feeds = [], podId = null) {
        const result = { numFeedUpdatedToReuse: 0 };
        if (!_.isEmpty(feeds)) {
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
                    result.numFeedUpdatedToReuse++;
                } catch (error) {
                    this.logger.warn(`Error to update to reuse the feed ${feedId} of userId ${userId} from pod ${podId}: ${error.message}`);
                }
            }
        }
        this.logger.debug('FeedsUpdated to reuse = %d', result.numFeedUpdatedToReuse);
        return result;
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
