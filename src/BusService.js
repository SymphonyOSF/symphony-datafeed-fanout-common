import _get from 'lodash/get';
import sqsSendMessageBatch from 'symphony-datafeed-core/sqsSendMessageBatch';
import broadcast from 'symphony-datafeed-core/broadcast';
import fanout from 'symphony-datafeed-core/fanout';
import sqsQueueCoordsFromName from 'symphony-datafeed-core/sqsQueueCoordsFromName';
import sqsSendMessage from 'symphony-datafeed-core/sqsSendMessage';

export default class BusService {

    constructor({ sqsClient, snsClient, options }) {
        this.sqsClient = sqsClient;
        this.snsClient = snsClient;

        this.aws = _get(options, 'aws');
        this.feedQueuePrefix = _get(options, 'fanout.feedQueuePrefix');
        this.topicNamePrefix = _get(options, 'fanout.messaging.broadcast.topicNamePrefix');
        this.subscriptionQueueName = _get(options, 'fanout.messaging.broadcastSubscriptionQueue.name');
        this.ingestionQueueName = _get(options, 'fanout.messaging.ingestionQueue.name');
        this.useQueueUrlBuilderFromAWS = _get(options, 'sqs.endpoint');
        this.telemetryIngestionQueueName = _get(options, 'telemetry.telemetryIngestionQueueName');

    }

    getIngestionQueueContext() {
        if (!this.ingestionQueueContext) {
            this.ingestionQueueContext = sqsQueueCoordsFromName(
                this.ingestionQueueName,
                {
                    aws: this.aws,
                    endpoint: this.useQueueUrlBuilderFromAWS
                }
            );
        }
        return this.ingestionQueueContext;
    }

    getTelemetryIngestionQueueContext() {
        if (!this.telemetryIngestionQueueContext) {
            this.telemetryIngestionQueueContext = sqsQueueCoordsFromName(
                this.telemetryIngestionQueueName,
                {
                    aws: this.aws,
                    endpoint: this.useQueueUrlBuilderFromAWS
                }
            );
        }
        return this.telemetryIngestionQueueContext;
    }

    sendTelemetry(telemetry) {
        const queueContext = this.getTelemetryIngestionQueueContext();
        return sqsSendMessage(this.sqsClient, queueContext, telemetry);
    }

    async pushBackMessage(message) {
        const queueContext = this.getIngestionQueueContext();

        /**
         * We could use here two functions to put the message back to the queue.
         * - sqsSendMessage
         * - sqsNackMessage (changeMessageVisibility)
         *
         * First, the function can receive a batch of messages (up to 10). Every message can be
         * delivered back to the ingestion queue, because not all of them can fail, so we can be
         * more efficient just sending back the message with problems, and we are doing this.
         *
         * We are sending the message back to the queue using sqsSendMessage because we are adding
         * a new attribute to the message, saying that this message has failed during the processing
         *
         * So, we can use this new attribute in metrics to know if there are messages failing during
         * the processing, giving some insight.
         *
         * In terms of costs there is no difference because nack or send count as a new request in
         * SQS. In therms of performance there is no difference as well, neither in terms of
         * latency.
         */

        return sqsSendMessage(this.sqsClient, queueContext, message);
    }

    async handleSplit(splits, messageSize) {
        const queueContext = this.getIngestionQueueContext();
        /**
         * the size of the batch is already considering the provided message size, e.g.:
         *  - splits array contains 7 messages
         *  - message size is 100k
         *  - number of messages in the batches will be: 2, 2, 2, 1
         */
        return sqsSendMessageBatch(this.sqsClient, queueContext, splits, messageSize);
    }

    fanoutMessage(message, feeds, podId) {
        return fanout(
            this.sqsClient,
            feeds,
            message,
            podId,
            {
                aws: this.aws,
                feedQueuePrefix: this.feedQueuePrefix,
                broadcast: {
                    topicNamePrefix: this.topicNamePrefix,
                    subscriptionQueueName: this.subscriptionQueueName
                },
                endpoint: this.useQueueUrlBuilderFromAWS
            }
        );
    }

    broadcastMessage(message, podId) {
        return broadcast(
            this.snsClient,
            podId,
            message,
            {
                aws: this.aws,
                broadcast: {
                    topicNamePrefix: this.topicNamePrefix
                }
            }
        );
    }

    sendRecycleFeed(feed) {
        const queueContext = this.getTelemetryIngestionQueueContext();
        return sqsSendMessage(this.sqsClient, queueContext, feed);
    }

}
