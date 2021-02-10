/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/* eslint-disable no-unused-expressions */
/* eslint-disable no-magic-numbers */
import { describe } from 'mocha';
import chai, { expect } from 'chai';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';
import chaiAsPromised from 'chai-as-promised';
import DF2Constants from 'symphony-datafeed-core/DF2Constants';

import DF2FanoutHandler from '../../src/DF2FanoutHandler';

chai.use(sinonChai);
chai.use(chaiAsPromised);

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
    const busService = {
        fanoutMessage: sinon.stub(),
        handleSplit: sinon.stub(),
        broadcastMessage: sinon.stub(),
        pushBackMessage: sinon.stub(),
        sendTelemetry: sinon.stub()
    };
    const databaseService = {
        fetchFeeds: sinon.stub()
    };
    const objectStorageService = {
        getPayload: sinon.stub()
    };
    const cacheRepoService = {
        set: sinon.stub()
    };
    const feedService = {
        removeFeeds: sinon.stub(),
        recyclingFeeds: sinon.stub(),
    };
    const hostname = 'host01';

    const logger = {
        debug: () => { },
        info: () => { },
        warn: () => { },
        error: () => { },
    };

    let df2FanoutService;

    beforeEach(() => {
        df2FanoutService = new DF2FanoutHandler({
            databaseService, busService, objectStorageService, feedService, cacheRepoService, options, hostname, logger
        });
    });

    afterEach(() => {
        objectStorageService.getPayload.reset();
        cacheRepoService.set.reset();
        databaseService.fetchFeeds.reset();
        busService.fanoutMessage.reset();
        busService.handleSplit.reset();
        busService.broadcastMessage.reset();
        busService.pushBackMessage.reset();
        busService.sendTelemetry.reset();
        feedService.removeFeeds.reset();
        feedService.recyclingFeeds.reset();
    });

    describe('Consume records', () => {

        describe('Adapt message errors', () => {

            it('Missing body property in the message', done => {
                const record = {
                    messageId: '1'
                };
                const records = [ record ];
                const result = df2FanoutService.consume(records);
                result.catch(error => {
                    expect(error.discardOriginalMessage).to.be.true;
                    expect(error.message).to.eq('Missing params in the message');
                    done();
                });
            });

            it('Missing messageId property in the message', done => {
                const record = {
                    body: { test: 1 }
                };
                const records = [ record ];
                const result = df2FanoutService.consume(records);
                result.catch(error => {
                    expect(error.discardOriginalMessage).to.be.true;
                    expect(error.message).to.eq('Missing params in the message');
                    done();
                });
            });

            it('Body has an invalid JSON', done => {
                const record = {
                    messageId: '1',
                    body: 'invalid'
                };
                const records = [ record ];
                const result = df2FanoutService.consume(records);
                result.then(data => {
                    expect(data[ 0 ].messageId).to.eq(record.messageId);
                    expect(data[ 0 ].isProcessed).to.be.true;
                    done();
                });
            });

            it('Message is not a raw message', done => {
                const rawBody = {
                    Message: {}
                };
                const record = {
                    messageId: '1',
                    body: JSON.stringify(rawBody)
                };
                const records = [ record ];
                const result = df2FanoutService.consume(records);
                result.then(data => {
                    expect(data[ 0 ].messageId).to.eq(record.messageId);
                    expect(data[ 0 ].isProcessed).to.be.true;
                    done();
                });
            });
        });
    });

    describe('Parse message errors', () => {

        it('Is a raw data but message data has an invalid json string', done => {
            const body = 'invalid';
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Message data attribute has an invalid payload', done => {
            const body = {
                payload: 0xFF
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Missing payload and s3 is returning an invalid payload', done => {
            const body = {};
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            objectStorageService.getPayload.resolves('invalid');
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });
    });

    describe('Messages allowed', () => {
        it('Not allowed payload type', done => {
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
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('JOIN_ROOM - pending is missing', done => {
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
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });
    });

    describe('Validation message errors', () => {

        it('Payload type is missing', done => {
            const payload = {};
            const body = {
                payload: Buffer.from(JSON.stringify(payload)).toString('base64')
            };
            const record = {
                messageId: '1',
                body: JSON.stringify(body)
            };
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Payload type is null', done => {
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
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Missing pod id', done => {
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
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Message is expired', done => {
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
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
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

        it('Sending broadcast message with success', done => {
            busService.broadcastMessage.resolves(true);
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Error sending broadcast message - discard original message', done => {
            busService.broadcastMessage.rejects(new Error('topic does not exist'));
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Error sending broadcast message - do not discard original message - push back ok', done => {
            busService.broadcastMessage.rejects(new Error('Any other error'));
            busService.pushBackMessage.resolves(true);
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Error sending broadcast message - do not discard original message - push back error', done => {
            busService.broadcastMessage.rejects(new Error('Any other error'));
            busService.pushBackMessage.rejects({ code: 'pushBackError' });
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.catch(error => {
                expect(error.code).to.eq('pushBackError');
                done();
            });
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

        it('Must split message - pull back ok', done => {
            busService.handleSplit.rejects({ message: 'error during sending splits' });
            busService.pushBackMessage.resolves(true);
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Must split message - pull back error', done => {
            busService.handleSplit.rejects({ message: 'error during sending splits' });
            busService.pushBackMessage.rejects({ code: 'pullBackError' });
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.catch(error => {
                expect(error.code).to.eq('pullBackError');
                done();
            });
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

        it('Fetch feeds is throwing an exception - pull back ok', done => {
            busService.pushBackMessage.resolves(true);
            databaseService.fetchFeeds.rejects({ message: 'error' });
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Fetch feeds is throwing an exception - pull back error', done => {
            busService.pushBackMessage.rejects({ code: 'pullBackError' });
            databaseService.fetchFeeds.rejects({ message: 'error' });
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.catch(error => {
                expect(error.code).to.eq('pullBackError');
                done();
            });
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

        it('Fanout feeds is returning only one promise rejected', done => {
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
            busService.pushBackMessage.resolves(true);
            databaseService.fetchFeeds.resolves(fetchFeedsResult);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Fanout feeds - ok', done => {
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
            databaseService.fetchFeeds.resolves(fetchFeedsResult);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.removeFeeds.resolves(removeFeedsResult);
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Fanout feeds - ok - telemetry exportation failing', done => {
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
            databaseService.fetchFeeds.resolves(fetchFeedsResult);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.removeFeeds.resolves(removeFeedsResult);
            busService.sendTelemetry.rejects({ error: 'error' });
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Fanout feeds - ok - more than one record', done => {
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
            databaseService.fetchFeeds.resolves(fetchFeedsResult);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.removeFeeds.resolves(removeFeedsResult);
            const records = [ record, record, record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Fanout feeds - reinserted message', done => {
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
            databaseService.fetchFeeds.resolves(fetchFeedsResult);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.removeFeeds.resolves(removeFeedsResult);
            const records = [ recordForReinsertedMessage ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(internalData.originalMessageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Fanout feeds - split message', done => {
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
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(tempRecord.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Fanout feeds - ok - removing stale feeds', done => {
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
            databaseService.fetchFeeds.resolves(fetchFeedsResult);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.removeFeeds.resolves(removeFeedsResult);
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Fanout feeds - ok - error in remove stale feeds', done => {
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
            databaseService.fetchFeeds.resolves(fetchFeedsResult);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.removeFeeds.rejects({ code: 'error removing stale feed' });
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
        });

        it('Fanout feeds - ok - but some queues does not exist yet', done => {
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
            databaseService.fetchFeeds.resolves(fetchFeedsResult);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.removeFeeds.rejects({ code: 'error removing stale feed' });
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                done();
            });
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

            it('Getting payload from s3 and do the fanout - empty payload returned from s3', done => {
                objectStorageService.getPayload.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'));
                databaseService.fetchFeeds.resolves(fetchFeedsResult);
                busService.fanoutMessage.resolves(fanoutFeedsResult);
                feedService.removeFeeds.resolves(removeFeedsResult);
                const records = [ record ];
                const result = df2FanoutService.consume(records);
                result.then(data => {
                    expect(data[ 0 ].messageId).to.eq(record.messageId);
                    expect(data[ 0 ].isProcessed).to.be.true;
                    done();
                });
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

            it('cutoff limit not reached - payload should go inside the message', done => {
                options.fanout.messaging.vlm.cutoffLimitPubToSqs = 20000;
                const df2FanoutServiceTemp = new DF2FanoutHandler({
                    databaseService, busService, objectStorageService, feedService, cacheRepoService: null, options, hostname, logger
                });
                objectStorageService.getPayload.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'));
                databaseService.fetchFeeds.resolves(fetchFeedsResult);
                busService.fanoutMessage.resolves(fanoutFeedsResult);
                feedService.removeFeeds.resolves(removeFeedsResult);
                const records = [ record ];
                const result = df2FanoutServiceTemp.consume(records);
                result.then(data => {
                    expect(data[ 0 ].messageId).to.eq(record.messageId);
                    expect(data[ 0 ].isProcessed).to.be.true;
                    done();
                });
            });

            it('cutoff limit is reached - payload should not go inside the message', done => {
                options.fanout.messaging.vlm.cutoffLimitPubToSqs = 10;
                const df2FanoutServiceTemp = new DF2FanoutHandler({
                    databaseService, busService, objectStorageService, feedService, cacheRepoService: null, options, hostname, logger
                });
                objectStorageService.getPayload.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'));
                databaseService.fetchFeeds.resolves(fetchFeedsResult);
                busService.fanoutMessage.resolves(fanoutFeedsResult);
                feedService.removeFeeds.resolves(removeFeedsResult);
                const records = [ record ];
                const result = df2FanoutServiceTemp.consume(records);
                result.then(data => {
                    expect(data[ 0 ].messageId).to.eq(record.messageId);
                    expect(data[ 0 ].isProcessed).to.be.true;
                    done();
                });
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

            it('Getting payload from s3 and do the fanout - valid payload from s3', done => {
                objectStorageService.getPayload.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'));
                cacheRepoService.set.resolves({ status: 'fulfilled' });
                databaseService.fetchFeeds.resolves(fetchFeedsResult);
                busService.fanoutMessage.resolves(fanoutFeedsResult);
                feedService.removeFeeds.resolves(removeFeedsResult);
                const records = [ record ];
                const result = df2FanoutService.consume(records);
                result.then(data => {
                    expect(data[ 0 ].messageId).to.eq(record.messageId);
                    expect(data[ 0 ].isProcessed).to.be.true;
                    done();
                });
            });

            it('Getting payload from s3 and do the fanout - valid payload from s3', done => {
                objectStorageService.getPayload.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'));
                cacheRepoService.set.rejects({ status: 'rejected' });
                databaseService.fetchFeeds.resolves(fetchFeedsResult);
                busService.fanoutMessage.resolves(fanoutFeedsResult);
                feedService.removeFeeds.resolves(removeFeedsResult);
                const records = [ record ];
                const result = df2FanoutService.consume(records);
                result.then(data => {
                    expect(data[ 0 ].messageId).to.eq(record.messageId);
                    expect(data[ 0 ].isProcessed).to.be.true;
                    done();
                });
            });

            it('Getting payload from s3 and do the fanout - cache repo is null', done => {
                const df2FanoutServiceTemp = new DF2FanoutHandler({
                    databaseService, busService, objectStorageService, feedService, cacheRepoService: null, options, hostname, logger
                });

                objectStorageService.getPayload.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'));
                databaseService.fetchFeeds.resolves(fetchFeedsResult);
                busService.fanoutMessage.resolves(fanoutFeedsResult);
                feedService.removeFeeds.resolves(removeFeedsResult);
                const records = [ record ];
                const result = df2FanoutServiceTemp.consume(records);
                result.then(data => {
                    expect(data[ 0 ].messageId).to.eq(record.messageId);
                    expect(data[ 0 ].isProcessed).to.be.true;
                    done();
                });
            });

            it('Getting payload from s3 and do the fanout - cutoff limit not reached', done => {
                options.fanout.messaging.vlm.cutoffLimitPubToSqs = 20000;
                const df2FanoutServiceTemp = new DF2FanoutHandler({
                    databaseService, busService, objectStorageService, feedService, cacheRepoService: null, options, hostname, logger
                });

                objectStorageService.getPayload.resolves(Buffer.from(JSON.stringify(payload)).toString('base64'));
                databaseService.fetchFeeds.resolves(fetchFeedsResult);
                busService.fanoutMessage.resolves(fanoutFeedsResult);
                feedService.removeFeeds.resolves(removeFeedsResult);
                const records = [ record ];
                const result = df2FanoutServiceTemp.consume(records);
                result.then(data => {
                    expect(data[ 0 ].messageId).to.eq(record.messageId);
                    expect(data[ 0 ].isProcessed).to.be.true;
                    done();
                });
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

        it('Should recycling feeds when have feedsItemsToBeDeleted', done => {
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
            databaseService.fetchFeeds.resolves(fetchedFeeds);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.recyclingFeeds.resolves();
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                expect(feedService.recyclingFeeds).to.have.been.calledOnce;
                done();
            });
        });

        it('Should recycling feeds when have feedsToBeStale', done => {
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
            databaseService.fetchFeeds.resolves(fetchedFeeds);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.recyclingFeeds.resolves();
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                expect(feedService.recyclingFeeds).to.have.been.calledOnce;
                done();
            });
        });

        it('Should recycling feeds when have feedsToBeReuse', done => {
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
            databaseService.fetchFeeds.resolves(fetchedFeeds);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.recyclingFeeds.resolves();
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                expect(feedService.recyclingFeeds).to.have.been.calledOnce;
                done();
            });
        });

        it('Should not recycling feeds when doenst have any feed to update the state', done => {
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
            databaseService.fetchFeeds.resolves(fetchedFeeds);
            busService.fanoutMessage.resolves(fanoutFeedsResult);
            feedService.recyclingFeeds.resolves();
            const records = [ record ];
            const result = df2FanoutService.consume(records);
            result.then(data => {
                expect(data[ 0 ].messageId).to.eq(record.messageId);
                expect(data[ 0 ].isProcessed).to.be.true;
                expect(feedService.recyclingFeeds).to.have.not.been.calledOnce;
                done();
            });
        });

    });
});
