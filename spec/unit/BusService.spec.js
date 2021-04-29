/* eslint-disable no-unused-expressions */
/* eslint-disable no-undef */
import { describe } from 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import BusService from '../../src/BusService';

describe('BusService Tests', () => {
    const message = { data: 'test' };
    const messageLength = JSON.stringify(message).length;
    const feeds = [];
    const podId = 123;
    const options = {};

    afterEach(() => {
        sinon.reset();
        sinon.restore();
        __rewire_reset_all__();
    });

    describe('Get ingestion queue context', () => {
        it('Should get the context with success', () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.returns({ url: 'test://test' }));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = busService.getIngestionQueueContext();
            expect(result.url).to.eqls('test://test');
        });
        it('Should not get the context', () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.throws(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = busService.getIngestionQueueContext();
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
        it('The queue context should be cached', () => {
            const fake = sinon.fake.returns({ url: 'test://test' });
            BusService.__Rewire__('sqsQueueCoordsFromName', fake);
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result = busService.getIngestionQueueContext();
            result = busService.getIngestionQueueContext();
            expect(result.url).to.eqls('test://test');
            expect(fake).to.be.calledOnce;
        });
    });

    describe('Pull back message', () => {
        it('Should pull back the message with success', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.returns({}));
            BusService.__Rewire__('sqsSendMessage', sinon.fake.resolves({ code: 'ok' }));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.pushBackMessage(message);
            expect(result.code).to.equals('ok');
        });
        it('Should not pull back message because get ingestion context is failing', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.throws(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.pushBackMessage(message);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
        it('Should not pull back message because send message is failing', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.returns({}));
            BusService.__Rewire__('sqsSendMessage', sinon.fake.rejects(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.pushBackMessage(message);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
    });

    describe('Send telemetry', () => {
        it('Should send telemetry with success', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.returns({}));
            BusService.__Rewire__('sqsSendMessage', sinon.fake.resolves({ code: 'ok' }));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.sendTelemetry(message);
            expect(result.code).to.equals('ok');
        });
        it('Should not send telemetry because get ingestion context is failing', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.throws(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.sendTelemetry(message);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
        it('Should not send telemetry because send message is failing', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.returns({}));
            BusService.__Rewire__('sqsSendMessage', sinon.fake.rejects(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.sendTelemetry(message);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
    });

    describe('Handle split', () => {
        it('Should send the split messages with success', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.returns({}));
            BusService.__Rewire__('sqsSendMessageBatch', sinon.fake.resolves({ code: 'ok' }));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.handleSplit([ message ], messageLength);
            expect(result.code).to.equals('ok');
        });
        it('Should not not send the split message because get ingestion context is failing', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.throws(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.handleSplit([ message ], messageLength);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
        it('Should not not send the split message because send message in batch is failing', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.returns({}));
            BusService.__Rewire__('sqsSendMessageBatch', sinon.fake.rejects(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.handleSplit([ message ], messageLength);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
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
            BusService.__Rewire__('broadcast', sinon.fake.resolves({ code: 'ok' }));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.broadcastMessage(message, podId);
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
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.returns({}));
            BusService.__Rewire__('sqsSendMessage', sinon.fake.resolves({ code: 'ok' }));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            const result = await busService.sendRecycleFeed(message);
            expect(result.code).to.equals('ok');
        });
        it('Should not send recycling feed because get ingestion context is failing', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.throws(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.sendRecycleFeed(message);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
        it('Should not send recycling feed because send message is failing', async () => {
            BusService.__Rewire__('sqsQueueCoordsFromName', sinon.fake.returns({}));
            BusService.__Rewire__('sqsSendMessage', sinon.fake.rejects(new Error('42')));
            const busService = new BusService({ sqsClient: null, snsClient: null, options });
            let result;
            try {
                result = await busService.sendRecycleFeed(message);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
    });
});
