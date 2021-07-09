/* eslint-disable no-unused-expressions */
/* eslint-disable no-undef */
import { describe } from 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import BusService from '../../src/BusService';

describe('BusService Tests', () => {
    const message = { data: 'test' };
    const messageSize = JSON.stringify(message).length;
    const feeds = [];
    const podId = 123;
    const options = {
        aws: { region: 'us-east-1', accountId: 1234567890 },
        fanout: {
            feedQueuePrefix: 'dev-df2feed',
            messaging: {
                broadcast: {
                    topicNamePrefix: 'dev-df2braodcast'
                },
                ingestionQueue: { name: 'dev-df2-ingestion' },
            },
        },
        supportServices: { queueName: 'dev-df2-support' },
    };

    afterEach(() => {
        sinon.reset();
        sinon.restore();
        __rewire_reset_all__();
    });

    describe('Pull back message', () => {
        it('Should pull back the message with success', async () => {
            const fake = sinon.fake.resolves({ code: 'ok' });
            BusService.__Rewire__('corePushBackMessage', fake);
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.pushBackMessage(message);
            expect(fake).to.be.calledOnceWith({
                sqsClient: null,
                queueName: options.fanout.messaging.ingestionQueue.name,
                message,
                options: { aws: options.aws },
            });
            expect(result.code).to.equals('ok');
        });
    });

    describe('Send telemetry', () => {
        it('Should send telemetry with success', async () => {
            const fake = sinon.fake.resolves({ code: 'ok' });
            BusService.__Rewire__('coreSupport', {
                sendSupportMessage: fake,
            });
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.sendTelemetry(message);
            expect(fake).to.be.calledOnceWith({
                sqsClient: null,
                queueName: options.supportServices.queueName,
                message,
                options: { aws: options.aws },
            });
            expect(result.code).to.equals('ok');
        });
    });

    describe('Handle split', () => {
        it('Should send the split messages with success', async () => {
            const fake = sinon.fake.resolves({ code: 'ok' });
            BusService.__Rewire__('reingestMessages', fake);
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.handleSplit([ message ], messageSize);
            expect(fake).to.be.calledOnceWith({
                sqsClient: null,
                queueName: options.fanout.messaging.ingestionQueue.name,
                messages: [ message ],
                sizeOfEveryMessage: messageSize,
                options: { aws: options.aws },
            });
            expect(result.code).to.equals('ok');
        });
    });

    describe('Fanout message', () => {
        it('Should fanout a message with success', async () => {
            BusService.__Rewire__('fanout', sinon.fake.resolves({ code: 'ok' }));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.fanoutMessage(message, feeds, podId);
            expect(result.code).to.equals('ok');
        });
        it('Should not fanout a message', async () => {
            BusService.__Rewire__('fanout', sinon.fake.rejects(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.fanoutMessage(message, feeds, podId);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
    });

    describe('Broadcast message', () => {
        it('Should broadcast a message with success', async () => {
            const fake = sinon.fake.resolves({ code: 'ok' });
            BusService.__Rewire__('broadcast', fake);
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.broadcastMessage(message, podId);
            expect(fake).to.be.calledOnceWith({
                snsClient: null,
                message,
                podId,
                options: {
                    aws: options.aws,
                    broadcast: options.fanout.messaging.broadcast,
                },
            });
            expect(result.code).to.equals('ok');
        });
        it('Should not broadcast a message', async () => {
            BusService.__Rewire__('broadcast', sinon.fake.rejects(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.broadcastMessage(message, podId);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
    });

    describe('Send Recycling Feed', () => {
        it('Should send recycling feed with success', async () => {
            const fake = sinon.fake.returns({ code: 'ok' });
            BusService.__Rewire__('coreSupport', {
                sendSupportMessage: fake,
            });
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.sendRecycleFeed(message);
            expect(fake).to.be.calledOnceWith({
                sqsClient: null,
                queueName: options.supportServices.queueName,
                message,
                options: { aws: options.aws },
            });
            expect(result.code).to.equals('ok');
        });
    });
});
