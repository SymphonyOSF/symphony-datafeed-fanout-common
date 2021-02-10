/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/* eslint-disable no-unused-expressions */
/* eslint-disable no-magic-numbers */
import { describe } from 'mocha';
import chai, { expect } from 'chai';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';
import chaiAsPromised from 'chai-as-promised';

import BusService from '../../src/BusService';

chai.use(sinonChai);
chai.use(chaiAsPromised);

describe('BusService Tests', () => {

    const sqsClient = sinon.stub();
    const snsClient = sinon.stub();
    const sqsSendMessageBatch = sinon.stub();
    const broadcast = sinon.stub();
    const fanout = sinon.stub();
    const sqsQueueCoordsFromName = sinon.stub();
    const sqsSendMessage = sinon.stub();
    const message = { data: 'test' };
    const messageLength = JSON.stringify(message).length;
    const feeds = [];
    const podId = 123;
    const options = {};

    let busService;

    beforeEach(() => {
        BusService.__Rewire__('sqsSendMessageBatch', sqsSendMessageBatch);
        BusService.__Rewire__('broadcast', broadcast);
        BusService.__Rewire__('fanout', fanout);
        BusService.__Rewire__('sqsQueueCoordsFromName', sqsQueueCoordsFromName);
        BusService.__Rewire__('sqsSendMessage', sqsSendMessage);

        busService = new BusService({ sqsClient, snsClient, options });
    });

    afterEach(() => {
        sqsSendMessageBatch.reset();
        broadcast.reset();
        fanout.reset();
        sqsQueueCoordsFromName.reset();
        sqsSendMessage.reset();
    });

    describe('Get ingestion queue context', () => {

        it('Should get the context with success', async () => {
            sqsQueueCoordsFromName.returns({ context: 'test-context' });
            const result = await busService.getIngestionQueueContext();
            expect(result.context).to.eq('test-context');
        });

        it('Should not get the context', async () => {
            sqsQueueCoordsFromName.returns(new Error('error'));
            try {
                await busService.getIngestionQueueContext();
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

    describe('Get ingestion queue context', () => {

        it('Should get the context with success', async () => {
            sqsQueueCoordsFromName.returns({ context: 'test-context' });
            const result = await busService.getIngestionQueueContext();
            expect(result.context).to.eq('test-context');
        });

        it('Should get the context with success', async () => {
            busService.ingestionQueueContext = { context: 'new-context' };
            const result = await busService.getIngestionQueueContext();
            expect(result.context).to.eq('new-context');
        });

        it('Should not get the context', async () => {
            sqsQueueCoordsFromName.returns(new Error('error'));
            try {
                await busService.getIngestionQueueContext();
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

    describe('Pull back message', () => {

        it('Should pull back the message with success', async () => {
            sqsQueueCoordsFromName.returns({ context: 'test-context' });
            sqsSendMessage.resolves({ code: 'ok' });
            const result = await busService.pushBackMessage(message);
            expect(result.code).to.eq('ok');
        });

        it('Should not pull back message because get ingestion context is failing', async () => {
            sqsQueueCoordsFromName.returns(new Error('error'));
            try {
                await busService.pushBackMessage(message);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
        it('Should not pull back message because send message is failing', async () => {
            sqsQueueCoordsFromName.returns({ context: 'test-context' });
            sqsSendMessage.rejects({ message: 'error' });
            try {
                await busService.pushBackMessage(message);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

    describe('Send telemetry', () => {

        it('Should send telemetry with success', async () => {
            sqsQueueCoordsFromName.returns({ context: 'test-context' });
            sqsSendMessage.resolves({ code: 'ok' });
            const result = await busService.sendTelemetry(message);
            expect(result.code).to.eq('ok');
        });

        it('Should not send telemetry because get ingestion context is failing', async () => {
            sqsQueueCoordsFromName.returns(new Error('error'));
            try {
                await busService.sendTelemetry(message);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
        it('Should not send telemetry because send message is failing', async () => {
            sqsQueueCoordsFromName.returns({ context: 'test-context' });
            sqsSendMessage.rejects({ message: 'error' });
            try {
                await busService.sendTelemetry(message);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

    describe('Handle split', () => {

        it('Should send the split messages with success', async () => {
            sqsQueueCoordsFromName.returns({ context: 'test-context' });
            sqsSendMessageBatch.resolves({ code: 'ok' });
            const result = await busService.handleSplit([ message ], messageLength);
            expect(result.code).to.eq('ok');
        });

        it('Should not not send the split message because get ingestion context is failing', async () => {
            sqsQueueCoordsFromName.returns(new Error('error'));
            try {
                await busService.handleSplit([ message ], messageLength);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
        it('Should not not send the split message because send message in batch is failing', async () => {
            sqsQueueCoordsFromName.returns({ context: 'test-context' });
            sqsSendMessageBatch.rejects({ message: 'error' });
            try {
                await busService.handleSplit([ message ], messageLength);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

    describe('Fanout message', () => {

        it('Should fanout a message with success', async () => {
            fanout.resolves({ code: 'ok' });
            const result = await busService.fanoutMessage(message, feeds, podId);
            expect(result.code).to.eq('ok');
        });

        it('Should not fanout a message', async () => {
            fanout.rejects({ message: 'error' });
            try {
                await busService.fanoutMessage(message, feeds, podId);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

    describe('Broadcast message', () => {

        it('Should broadcast a message with success', async () => {
            broadcast.resolves({ code: 'ok' });
            const result = await busService.broadcastMessage(message, podId);
            expect(result.code).to.eq('ok');
        });

        it('Should not broadcast a message', async () => {
            broadcast.rejects({ message: 'error' });
            try {
                await busService.broadcastMessage(message, podId);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

    describe('Send Recycling Feed', () => {

        it('Should send recycling feed with success', async () => {
            sqsQueueCoordsFromName.resolves({ context: 'recycling-context' });
            sqsSendMessage.resolves({ code: 'ok' });
            const result = await busService.sendRecycleFeed(message);
            expect(result.code).to.eq('ok');
        });

        it('Should not send recycling feed because get ingestion context is failing', async () => {
            sqsQueueCoordsFromName.returns(new Error('error'));
            try {
                await busService.sendRecycleFeed(message);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });

        it('Should not send recycling feed because send message is failing', async () => {
            sqsQueueCoordsFromName.resolves({ context: 'recycling-context' });
            sqsSendMessage.rejects({ message: 'error' });
            try {
                await busService.sendRecycleFeed(message);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });
});
