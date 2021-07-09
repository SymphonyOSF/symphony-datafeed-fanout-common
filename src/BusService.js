import {
    feeds as coreFeeds,
    support as coreSupport,
} from 'symphony-datafeed-core';

const {
    broadcast,
    fanout,
    pushBackMessage: corePushBackMessage,
    reingestMessages,
} = coreFeeds.producers;

export default class BusService {

    /**
     * Constructor
     *
     * @param {{
     *  sqsClient: AWS.SQS,
     *  snsClient: AWS.SNS,
     *  options: {
     *      aws: {region: string, accountId: number},
     *      fanout: {
     *          feedQueuePrefix: string,
     *          messaging: {
     *              broadcast: {topicNamePrefix: string}
     *          },
     *          ingestionQueue: {name: string}
     *      },
     *      supportServices: {queueName: string}
     *  }
     * }} param
     */
    constructor({ sqsClient, snsClient, options }) {
        this.sqsClient = sqsClient;
        this.snsClient = snsClient;
        const {
            aws,
            fanout: {
                feedQueuePrefix,
                messaging: {
                    broadcast: {
                        topicNamePrefix,
                    },
                    ingestionQueue,
                },
            },
            sqs,
            supportServices,
        } = options;

        this.aws = aws;
        this.fanoutOptions = { feedQueuePrefix, aws };
        this.broadcastOptions = { aws, broadcast: { topicNamePrefix } };
        this.ingestionQueueName = ingestionQueue.name;
        this.localstackSqsEndpoint = sqs?.endpoint;
        this.supportServicesQueueName = supportServices.queueName;
    }

    sendTelemetry(telemetryMessage) {
        return coreSupport.sendSupportMessage({
            sqsClient: this.sqsClient,
            queueName: this.supportServicesQueueName,
            message: telemetryMessage,
            options: { aws: this.aws },
        });
    }

    pushBackMessage(message) {
        /*
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
         * SQS. In terms of performance there is no difference as well, neither in terms of
         * latency.
         */
        return corePushBackMessage({
            sqsClient: this.sqsClient,
            queueName: this.ingestionQueueName,
            message,
            options: { aws: this.aws },
        });
    }

    handleSplit(splits, messageSize) {
        /*
         * the size of the batch is already considering the provided message size, e.g.:
         *  - splits array contains 7 messages
         *  - message size is 100k
         *  - number of messages in the batches will be: 2, 2, 2, 1
         */
        return reingestMessages({
            sqsClient: this.sqsClient,
            queueName: this.ingestionQueueName,
            messages: splits,
            sizeOfEveryMessage: messageSize,
            options: { aws: this.aws },
        });
    }

    fanoutMessage(message, feeds, podId) {
        return fanout({
            sqsClient: this.sqsClient,
            feeds,
            message,
            podId,
            options: this.fanoutOptions,
        });
    }

    broadcastMessage(message, podId) {
        return broadcast({
            snsClient: this.snsClient,
            podId,
            message,
            options: this.broadcastOptions,
        });
    }

    sendRecycleFeed(feed) {
        return coreSupport.sendSupportMessage({
            sqsClient: this.sqsClient,
            queueName: this.supportServicesQueueName,
            message: feed,
            options: { aws: this.aws },
        });
    }

}
