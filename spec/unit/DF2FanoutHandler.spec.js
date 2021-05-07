/* eslint-disable no-magic-numbers */
/* eslint-disable no-unused-expressions */
/* eslint-disable no-undef */
import { describe } from 'mocha';
import chai, { expect } from 'chai';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';

chai.use(sinonChai);

import { DF2Constants } from 'symphony-datafeed-core';

import DF2FanoutHandler from '../../src/DF2FanoutHandler';

describe('DF2FanoutHandler Tests', () => {
    const options = {
        environment: 'dev',
        cache: {
            type: 'redis',
            itemExpirationTimeInSec: 604800
        },
        fanout: {
            messaging: {
                vlm: {
                    cutoffLimitPubToSqs: 10
                },
            },
            splitting: {
                startSplitFromRoomSize: 3,
                distributionListSizeOfSplitMsg: 2
            }
        },
        telemetry: {
            isExportationActivated: 1
        }
    };
    const logger = {
        debug: sinon.fake(),
        info: sinon.fake(),
        warn: sinon.fake(),
        error: sinon.fake(),
    };

    afterEach(() => {
        sinon.reset();
        sinon.restore();
    });

    describe('Consume records', () => {
        describe('Adapt message errors', () => {
            it('Missing body property in the message', async () => {
                const fanoutService = new DF2FanoutHandler({ logger });
                const record = {
                    messageId: '1'
                };
                const records = [ record ];
                let result;
                try {
                    result = await fanoutService.consume(records);
                } catch (error) {
                    expect(error.discardOriginalMessage).to.be.true;
                    expect(error.message).to.eq('Missing params in the message');
                }
                expect(result).to.be.undefined;
            });
            it('Missing messageId property in the message', async () => {
                const fanoutService = new DF2FanoutHandler({ logger });
                const record = {
                    body: { test: 1 }
                };
                const records = [ record ];
                let result;
                try {
                    result = await fanoutService.consume(records);
                } catch (error) {
                    expect(error.discardOriginalMessage).to.be.true;
                    expect(error.message).to.eq('Missing params in the message');
                }
                expect(result).to.be.undefined;
            });
            it('Body has an invalid JSON - should ignore and ack', async () => {
                const fanoutService = new DF2FanoutHandler({ logger });
                const record = {
                    messageId: '1',
                    body: 'invalid'
                };
                const records = [ record ];
                const result = await fanoutService.consume(records);
                expect(result[ 0 ].data).to.be.undefined;
                expect(result[ 0 ].messageId).to.eq(record.messageId);
                expect(result[ 0 ].isProcessed).to.be.true;
            });
            it('Message is not a raw message - should ignore and ack', async () => {
                const fanoutService = new DF2FanoutHandler({ logger });
                const rawBody = {
                    Message: {}
                };
                const record = {
                    messageId: '1',
                    body: JSON.stringify(rawBody)
                };
                const records = [ record ];
                const result = await fanoutService.consume(records);
                expect(result[ 0 ].data).to.be.undefined;
                expect(result[ 0 ].messageId).to.eq(record.messageId);
                expect(result[ 0 ].isProcessed).to.be.true;
            });
        });
    });

    describe('Parse message errors', () => {
        it('Is a raw data but message data has an invalid json string', async () => {
            const fanoutService = new DF2FanoutHandler({ logger });
            const body = 'invalid';
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });

        it('Message data attribute has an invalid payload', async () => {
            const fanoutService = new DF2FanoutHandler({ logger });
            const body = {
                payload: 0xFF
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });

        it('Missing payload and s3 is returning an invalid payload', async () => {
            const objectStorageService = {
                getPayload: sinon.fake.resolves('invalid')
            };
            const fanoutService = new DF2FanoutHandler({ logger, objectStorageService });
            const body = {};
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
    });

    describe('Messages allowed', () => {
        it('Not allowed payload type', async () => {
            const fanoutService = new DF2FanoutHandler({ logger });
            const payload = {
                podId: 1,
                payloadType: 'com.symphony.s2.model.chat.MaestroMessage.INVALID'
            };
            const body = {
                payload: Buffer.from(JSON.stringify(payload)).toString('base64')
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });

        it('JOIN_ROOM - pending is missing', async () => {
            const busService = {
                pushBackMessage: sinon.fake.resolves()
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService });
            const payload = {
                podId: 1,
                payloadType: 'com.symphony.s2.model.chat.MaestroMessage.JOIN_ROOM',
                payload: {}
            };
            const body = {
                payload: Buffer.from(JSON.stringify(payload)).toString('base64')
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
    });

    describe('Validation message errors', () => {
        it('Payload type is missing', async () => {
            const fanoutService = new DF2FanoutHandler({ logger });
            const payload = {};
            const body = {
                payload: Buffer.from(JSON.stringify(payload)).toString('base64')
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Payload type is null', async () => {
            const fanoutService = new DF2FanoutHandler({ logger });
            const payload = {
                payloadType: null
            };
            const body = {
                payload: Buffer.from(JSON.stringify(payload)).toString('base64')
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Missing pod id', async () => {
            const fanoutService = new DF2FanoutHandler({ logger });
            const payload = {
                payloadType: 'com.symphony.s2.model.chat.SocialMessage'
            };
            const body = {
                payload: Buffer.from(JSON.stringify(payload)).toString('base64')
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Message is expired', async () => {
            const fanoutService = new DF2FanoutHandler({ logger });
            const payload = {
                payloadType: 'com.symphony.s2.model.chat.Typing',
                podId: 1,
                expirationDate: '2000-12-16T19:28:54.888Z'
            };
            const body = {
                payload: Buffer.from(JSON.stringify(payload)).toString('base64')
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
    });

    describe('Broadcast message', () => {
        const payload = {
            payloadType: 'com.symphony.s2.model.chat.Typing',
            podId: 1,
            broadcast: 'ALL'
        };
        const body = {
            payload: Buffer.from(JSON.stringify(payload)).toString('base64')
        };
        const record = {
            messageId: '1',
            body: JSON.stringify(body)
        };
        it('Sending broadcast message with success', async () => {
            const busService = {
                broadcastMessage: sinon.fake.resolves()
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Error sending broadcast message - discard original message', async () => {
            const busService = {
                broadcastMessage: sinon.fake.rejects('topic does not exist')
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Error sending broadcast message - do not discard original message - push back ok', async () => {
            const busService = {
                broadcastMessage: sinon.fake.rejects('Any other error'),
                pushBackMessage: sinon.fake.resolves(true)
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Error sending broadcast message - do not discard original message - push back error', async () => {
            const busService = {
                broadcastMessage: sinon.fake.rejects('Any other error'),
                pushBackMessage: sinon.fake.rejects('Pushback Error')
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService });
            const records = [ record ];
            let result;
            try {
                result = await fanoutService.consume(records);
            } catch (error) {
                expect(error.message).to.equals('Pushback Error');
            }
            expect(result).to.be.undefined;
        });
    });

    describe('Error splitting the message', () => {
        const payload = {
            payloadType: 'com.symphony.s2.model.chat.Typing',
            podId: 1,
            distributionList: [ 1, 2, 3, 4, 5, 6, 7 ]
        };
        const body = {
            payload: Buffer.from(JSON.stringify(payload)).toString('base64')
        };
        const record = {
            messageId: '1',
            body: JSON.stringify(body)
        };
        it('Must split message - pull back ok', async () => {
            const busService = {
                handleSplit: sinon.fake.rejects('error during sending splits'),
                pushBackMessage: sinon.fake.resolves(true)
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Must split message - pull back error', async () => {
            const busService = {
                handleSplit: sinon.fake.rejects('error during sending splits'),
                pushBackMessage: sinon.fake.rejects('Pushback Error')
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService });
            const records = [ record ];
            let result;
            try {
                result = await fanoutService.consume(records);
            } catch (error) {
                expect(error.message).to.equals('Pushback Error');
            }
            expect(result).to.be.undefined;
        });
    });

    describe('Fetch feeds', () => {
        const payload = {
            payloadType: 'com.symphony.s2.model.chat.Typing',
            podId: 1,
            distributionList: [ 1 ]
        };
        const body = {
            payload: Buffer.from(JSON.stringify(payload)).toString('base64')
        };
        const record = {
            messageId: '1',
            body: JSON.stringify(body)
        };
        it('Fetch feeds is throwing an exception - pull back ok', async () => {
            const busService = {
                pushBackMessage: sinon.fake.resolves(true)
            };
            const databaseService = {
                fetchFeeds: sinon.fake.rejects('error')
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService, databaseService });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Fetch feeds is throwing an exception - pull back error', async () => {
            const busService = {
                pushBackMessage: sinon.fake.rejects('Pushback Error')
            };
            const databaseService = {
                fetchFeeds: sinon.fake.rejects('error')
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService, databaseService });
            const records = [ record ];
            let result;
            try {
                result = await fanoutService.consume(records);
            } catch (error) {
                expect(error.message).to.equals('Pushback Error');
            }
            expect(result).to.be.undefined;
        });
    });

    describe('Fanout feeds', () => {
        const dateNow = Date.now();
        const payload = {
            createdDate: new Date(dateNow - 500).toISOString(),
            notificationDate: new Date(dateNow - 300).toISOString(),
            payloadType: 'com.symphony.s2.model.chat.Typing',
            podId: 1,
            distributionList: [ 1 ]
        };
        const body = {
            payload: Buffer.from(JSON.stringify(payload)).toString('base64')
        };
        const record = {
            messageId: '1',
            body: JSON.stringify(body),
            attributes: {
                SentTimestamp: dateNow - 100,
            }
        };
        it('Fanout feeds is returning only one promise rejected', async () => {
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: []
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'rejected',
                    message: 'error fetching feed'
                }
            ];
            const busService = {
                pushBackMessage: sinon.fake.resolves(true),
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult)
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
            };
            const fanoutService = new DF2FanoutHandler({ logger, busService, databaseService });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Fanout feeds - ok', async () => {
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1',
                    },
                    {
                        feedId: '2',
                    }
                ],
                feedsItemsToBeDeleted: [
                    {
                        feedId: '3',
                    },
                    {
                        feedId: '4',
                    }
                ]
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const removeFeedsResult = [];
            const busService = {
                pushBackMessage: sinon.fake.resolves(true),
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult)
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
            };
            const feedService = {
                removeFeeds: sinon.fake.resolves(removeFeedsResult)
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Fanout feeds - ok - telemetry exportation failing', async () => {
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: []
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const removeFeedsResult = [];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                sendTelemetry: sinon.fake.rejects('error')
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
            };
            const feedService = {
                removeFeeds: sinon.fake.resolves(removeFeedsResult)
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Fanout feeds - ok - more than one record', async () => {
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: []
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const removeFeedsResult = [];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
            };
            const feedService = {
                removeFeeds: sinon.fake.resolves(removeFeedsResult)
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record, record, record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Fanout feeds - reinserted message', async () => {
            const internalData = {
                originalMessageId: '1',
                isSplit: true, // this is the identification for a reinserted message
                payload: {
                    payloadType: 'com.symphony.s2.model.chat.SocialMessage',
                    podId: 1,
                    distributionList: [ 1 ],
                    payload: {
                        _type: 'com.symphony.s2.model.chat.SocialMessage',
                        messageId: 'VLfRsETA5F+WnqcIZTmbLn///ov4Ue4pZQ=='
                    }
                }
            };
            const reinsertedPayload = {
                Message: JSON.stringify(internalData)
            };
            const recordForReinsertedMessage = {
                messageId: '2',
                body: JSON.stringify(reinsertedPayload)
            };

            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: []
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const removeFeedsResult = [];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
            };
            const feedService = {
                removeFeeds: sinon.fake.resolves(removeFeedsResult)
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ recordForReinsertedMessage ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(internalData.originalMessageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Fanout feeds - split message', async () => {
            const busService = {
                pushBackMessage: sinon.fake.resolves(),
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService
            });
            const tempData = {
                payloadType: 'com.symphony.s2.model.chat.SocialMessage',
                podId: 1,
                distributionList: [ 1, 2, 3, 4, 5, 6, 7 ]
            };
            const tempBody = {
                payload: Buffer.from(JSON.stringify(tempData)).toString('base64')
            };
            const tempRecord = {
                messageId: '1',
                body: JSON.stringify(tempBody)
            };
            const records = [ tempRecord ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(tempRecord.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Fanout feeds - ok - removing stale feeds', async () => {
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: [
                    'one'
                ]
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const removeFeedsResult = [
                [ 'bla' ],
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                pushBackMessage: sinon.fake.resolves()
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
            };
            const feedService = {
                removeFeeds: sinon.fake.resolves(removeFeedsResult)
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Fanout feeds - ok - error in remove stale feeds', async () => {
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: [
                    'one'
                ]
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                pushBackMessage: sinon.fake.resolves()
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
            };
            const feedService = {
                removeFeeds: sinon.fake.rejects('error')
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
        it('Fanout feeds - ok - but some queues does not exist yet', async () => {
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: [
                    'one'
                ]
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    reason: 'ok'
                },
                {
                    status: 'rejected',
                    reason: {
                        message: DF2Constants.Errors.ERROR_NONEXISTENT_QUEUE
                    }
                }
            ];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                pushBackMessage: sinon.fake.resolves()
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
            };
            const feedService = {
                removeFeeds: sinon.fake.rejects('error')
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
        });
    });

    describe('Handle VLM', () => {
        describe('No payload', () => {
            const payload = {
                payloadType: 'com.symphony.s2.model.chat.SocialMessage',
                podId: 1,
                distributionList: [ 1 ]
            };
            const body = {};
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: []
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const removeFeedsResult = [];

            it('Getting payload from s3 and do the fanout - empty payload returned from s3', async () => {
                const busService = {
                    fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                };
                const databaseService = {
                    fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
                };
                const feedService = {
                    removeFeeds: sinon.fake.resolves(removeFeedsResult)
                };
                const objectStorageService = {
                    getPayload: sinon.fake.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'))
                };
                const fanoutService = new DF2FanoutHandler({
                    logger, busService, databaseService, feedService, objectStorageService
                });
                const records = [ record ];
                const result = await fanoutService.consume(records);
                expect(result[ 0 ].messageId).to.eq(record.messageId);
                expect(result[ 0 ].isProcessed).to.be.true;
            });
        });
        describe('No payload and retrieving a large DL in the payload fetched from S3', () => {
            const payload = {
                payloadType: 'com.symphony.s2.model.chat.SocialMessage',
                podId: 1,
                distributionList: [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ]
            };
            const body = {};
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: []
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const removeFeedsResult = [];

            it('cutoff limit not reached - payload should go inside the message', async () => {
                options.fanout.messaging.vlm.cutoffLimitPubToSqs = 20000;
                const busService = {
                    fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                };
                const databaseService = {
                    fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
                };
                const feedService = {
                    removeFeeds: sinon.fake.resolves(removeFeedsResult)
                };
                const objectStorageService = {
                    getPayload: sinon.fake.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'))
                };
                const fanoutService = new DF2FanoutHandler({
                    logger, busService, databaseService, feedService, objectStorageService
                });
                const records = [ record ];
                const result = await fanoutService.consume(records);
                expect(result[ 0 ].messageId).to.eq(record.messageId);
                expect(result[ 0 ].isProcessed).to.be.true;
            });
            it('cutoff limit is reached - payload should not go inside the message', async () => {
                options.fanout.messaging.vlm.cutoffLimitPubToSqs = 10;
                const busService = {
                    fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                };
                const databaseService = {
                    fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
                };
                const feedService = {
                    removeFeeds: sinon.fake.resolves(removeFeedsResult)
                };
                const objectStorageService = {
                    getPayload: sinon.fake.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'))
                };
                const fanoutService = new DF2FanoutHandler({
                    logger, busService, databaseService, feedService, objectStorageService
                });
                const records = [ record ];
                const result = await fanoutService.consume(records);
                expect(result[ 0 ].messageId).to.eq(record.messageId);
                expect(result[ 0 ].isProcessed).to.be.true;
            });
        });
        describe('With payload', () => {
            const payload = {
                payloadType: 'com.symphony.s2.model.chat.SocialMessage',
                podId: 1,
                distributionList: [ 1 ],
                payload: {
                    data: 'something'
                }
            };
            const body = {
                s3Key: 's3Key',
                s3BucketName: 's3BucketName'
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const fetchFeedsResult = {
                feeds: [
                    {
                        feedId: '1'
                    },
                    {
                        feedId: '2'
                    }
                ],
                feedsItemsToBeDeleted: []
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    message: 'ok'
                },
                {
                    status: 'fulfilled',
                    message: 'ok'
                }
            ];
            const removeFeedsResult = [];
            it('Getting payload from s3 and do the fanout - valid payload from s3 - cache resolves', async () => {
                const busService = {
                    fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                };
                const databaseService = {
                    fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
                };
                const feedService = {
                    removeFeeds: sinon.fake.resolves(removeFeedsResult)
                };
                const objectStorageService = {
                    getPayload: sinon.fake.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'))
                };
                const cacheRepoService = {
                    set: sinon.fake.resolves({ status: 'fulfilled' })
                };
                const fanoutService = new DF2FanoutHandler({
                    logger, busService, databaseService, feedService, objectStorageService, cacheRepoService
                });
                const records = [ record ];
                const result = await fanoutService.consume(records);
                expect(result[ 0 ].messageId).to.eq(record.messageId);
                expect(result[ 0 ].isProcessed).to.be.true;
            });
            it('Getting payload from s3 and do the fanout - valid payload from s3 - cache rejects', async () => {
                const busService = {
                    fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                };
                const databaseService = {
                    fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
                };
                const feedService = {
                    removeFeeds: sinon.fake.resolves(removeFeedsResult)
                };
                const objectStorageService = {
                    getPayload: sinon.fake.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'))
                };
                const cacheRepoService = {
                    set: sinon.fake.rejects('rejected')
                };
                const fanoutService = new DF2FanoutHandler({
                    logger, busService, databaseService, feedService, objectStorageService, cacheRepoService
                });
                const records = [ record ];
                const result = await fanoutService.consume(records);
                expect(result[ 0 ].messageId).to.eq(record.messageId);
                expect(result[ 0 ].isProcessed).to.be.true;
            });
            it('Getting payload from s3 and do the fanout - cache repo is null', async () => {
                const busService = {
                    fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                };
                const databaseService = {
                    fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
                };
                const feedService = {
                    removeFeeds: sinon.fake.resolves(removeFeedsResult)
                };
                const objectStorageService = {
                    getPayload: sinon.fake.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'))
                };
                const fanoutService = new DF2FanoutHandler({
                    logger, busService, databaseService, feedService, objectStorageService
                });
                const records = [ record ];
                const result = await fanoutService.consume(records);
                expect(result[ 0 ].messageId).to.eq(record.messageId);
                expect(result[ 0 ].isProcessed).to.be.true;
            });
            it('Getting payload from s3 and do the fanout - cutoff limit not reached', async () => {
                options.fanout.messaging.vlm.cutoffLimitPubToSqs = 20000;
                const busService = {
                    fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
                };
                const databaseService = {
                    fetchFeeds: sinon.fake.resolves(fetchFeedsResult)
                };
                const feedService = {
                    removeFeeds: sinon.fake.resolves(removeFeedsResult)
                };
                const objectStorageService = {
                    getPayload: sinon.fake.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'))
                };
                const fanoutService = new DF2FanoutHandler({
                    logger, busService, databaseService, feedService, objectStorageService
                });
                const records = [ record ];
                const result = await fanoutService.consume(records);
                expect(result[ 0 ].messageId).to.eq(record.messageId);
                expect(result[ 0 ].isProcessed).to.be.true;
            });
        });
    });

    describe('Recycling feeds', () => {
        const dateNow = Date.now();
        const payload = {
            createdDate: new Date(dateNow - 500).toISOString(),
            notificationDate: new Date(dateNow - 300).toISOString(),
            payloadType: 'com.symphony.s2.model.chat.Typing',
            podId: 1,
            distributionList: [ 1 ]
        };
        const body = {
            payload: Buffer.from(JSON.stringify(payload)).toString('base64')
        };
        const record = {
            messageId: '1',
            body: JSON.stringify(body),
            attributes: {
                SentTimestamp: dateNow - 100,
            }
        };
        it('Should recycling feeds when have feedsItemsToBeDeleted', async () => {
            const fetchedFeeds = {
                feeds: [
                    {
                        feedId: 1
                    }
                ],
                feedsItemsToBeDeleted: [
                    {
                        feedId: 2
                    }
                ]
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    reason: 'ok'
                }
            ];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchedFeeds)
            };
            const feedService = {
                recyclingFeeds: sinon.fake.resolves()
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
            expect(feedService.recyclingFeeds).to.have.been.calledOnce;
        });
        it('Should recycling feeds when have feedsToBeStale', async () => {
            const fetchedFeeds = {
                feeds: [
                    {
                        feedId: 1
                    }
                ],
                feedsToBeStale: [
                    {
                        feedId: 2
                    }
                ]
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    reason: 'ok'
                }
            ];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchedFeeds)
            };
            const feedService = {
                recyclingFeeds: sinon.fake.resolves()
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
            expect(feedService.recyclingFeeds).to.have.been.calledOnce;
        });
        it('Should recycling feeds when have feedsToBeReuse', async () => {
            const fetchedFeeds = {
                feeds: [
                    {
                        feedId: 1
                    }
                ],
                feedsToBeReuse: [
                    {
                        feedId: 2
                    }
                ]
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    reason: 'ok'
                }
            ];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchedFeeds)
            };
            const feedService = {
                recyclingFeeds: sinon.fake.resolves()
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
            expect(feedService.recyclingFeeds).to.have.been.calledOnce;
        });
        it('Should not recycling feeds when doenst have any feed to update the state', async () => {
            const fetchedFeeds = {
                feeds: [
                    {
                        feedId: 1
                    }
                ],
            };
            const fanoutFeedsResult = [
                {
                    status: 'fulfilled',
                    reason: 'ok'
                }
            ];
            const busService = {
                fanoutMessage: sinon.fake.resolves(fanoutFeedsResult),
            };
            const databaseService = {
                fetchFeeds: sinon.fake.resolves(fetchedFeeds)
            };
            const feedService = {
                recyclingFeeds: sinon.fake.resolves()
            };
            const fanoutService = new DF2FanoutHandler({
                logger, busService, databaseService, feedService
            });
            const records = [ record ];
            const result = await fanoutService.consume(records);
            expect(result[ 0 ].messageId).to.eq(record.messageId);
            expect(result[ 0 ].isProcessed).to.be.true;
            expect(feedService.recyclingFeeds).to.have.not.been.calledOnce;
        });
    });
});
