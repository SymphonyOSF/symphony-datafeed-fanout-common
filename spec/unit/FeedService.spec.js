/* eslint-disable no-unused-expressions */
/* eslint-disable no-undef */
import { describe } from 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import FeedService from '../../src/FeedService';

describe('FeedService Tests', () => {
    afterEach(() => {
        sinon.reset();
        sinon.restore();
        __rewire_reset_all__();
    });

    const podId = 123;
    const sqsClient = null;
    const ddbDaxClient = null;
    const ddbDirectClient = null;
    const options = {};
    const oneFeed = [
        {
            feedId: '1',
            userId: 1,
            feedsKey: 'mt:df:f:u:1'
        }
    ];
    const logger = {
        warn: sinon.fake(),
        debug: sinon.fake(),
    };

    describe('Remove invalid feeds', () => {
        const busService = null;
        it('Should remove invalid feeds with success', async () => {
            const fakeResult = [ [ Promise.resolve() ], Promise.resolve() ];
            FeedService.__Rewire__('removeFeeds', sinon.fake.resolves(fakeResult));
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger
            });
            const result = await feedService.removeFeeds(oneFeed, podId);
            expect(result).to.equals(fakeResult);
            expect(logger.warn).to.not.have.been.called;
        });
        it('Remove feeds should return an rejection', async () => {
            FeedService.__Rewire__('removeFeeds', sinon.fake.rejects(new Error('42')));
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger
            });
            let result;
            try {
                result = await feedService.removeFeeds(oneFeed, podId);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
            expect(logger.warn).to.not.have.been.called;
        });
        it('Remove feeds should properly log removals - 1', async () => {
            const localLogger = {
                warn: sinon.fake(),
                debug: sinon.fake(),
            };
            const fakeResult = [ [ { status: 'rejected' } ], { status: 'rejected' } ];
            FeedService.__Rewire__('removeFeeds', sinon.fake.resolves(fakeResult));
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger: localLogger
            });
            await feedService.removeFeeds(oneFeed, podId);
            expect(localLogger.warn).to.have.been.calledTwice;
        });
        it('Remove feeds should properly log removals - 2', async () => {
            const localLogger = {
                warn: sinon.fake(),
                debug: sinon.fake(),
            };
            const fakeResult = [ [ { status: 'rejected' } ], { status: 'fulfilled' } ];
            FeedService.__Rewire__('removeFeeds', sinon.fake.resolves(fakeResult));
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger: localLogger
            });
            await feedService.removeFeeds(oneFeed, podId);
            expect(localLogger.warn).to.have.been.calledOnce;
        });
        it('Remove feeds should properly log removals - 3', async () => {
            const localLogger = {
                warn: sinon.fake(),
                debug: sinon.fake(),
            };
            const fakeResult = [ [ { status: 'fulfilled' } ], { status: 'rejected' } ];
            FeedService.__Rewire__('removeFeeds', sinon.fake.resolves(fakeResult));
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger: localLogger
            });
            await feedService.removeFeeds(oneFeed, podId);
            expect(localLogger.warn).to.have.been.calledOnce;
        });
    });

    describe('Update feeds to be stale', () => {
        const busService = null;
        it('Should update the feeds to be stale', async () => {
            const localLogger = {
                warn: sinon.fake(),
                debug: sinon.fake(),
            };
            FeedService.__Rewire__('markFeedStale', sinon.fake.resolves());
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger: localLogger
            });
            const result = await feedService.updateFeedsToStale(oneFeed, podId);
            expect(result.numFeedUpdatedToStale).to.eqls(1);
            expect(localLogger.warn).to.have.not.been.called;
            expect(localLogger.debug).to.have.been.calledOnce;
        });
        it('Should fail update the feeds to be stale', async () => {
            const localLogger = {
                warn: sinon.fake(),
                debug: sinon.fake(),
            };
            FeedService.__Rewire__('markFeedStale', sinon.fake.rejects(new Error('42')));
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger: localLogger
            });
            const result = await feedService.updateFeedsToStale(oneFeed, podId);
            expect(result.numFeedUpdatedToStale).to.equals(0);
            expect(localLogger.warn).to.have.been.calledOnce;
            expect(localLogger.debug).to.have.been.calledOnce;
        });
        it('Should update one and fail another the feeds to be stale', async () => {
            const localLogger = {
                warn: sinon.fake(),
                debug: sinon.fake(),
            };
            const twoFeeds = [
                ...oneFeed,
                {
                    feedId: '2',
                    userId: 2,
                    feedsKey: 'mt:df:f:u:2'
                },
            ];
            const stubMarkFeedStale = sinon.stub();
            FeedService.__Rewire__('markFeedStale', stubMarkFeedStale);
            stubMarkFeedStale.onCall(0).resolves();
            stubMarkFeedStale.onCall(1).rejects('Fail to update feed');
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger: localLogger
            });
            const result = await feedService.updateFeedsToStale(twoFeeds, podId);
            expect(result.numFeedUpdatedToStale).to.be.eq(1);
            expect(localLogger.warn).to.have.been.calledOnce;
            expect(localLogger.debug).to.have.been.calledOnce;
        });
        it('Should do nothing when there no feeds', async () => {
            const fakeMarkFeedStale = sinon.fake();
            FeedService.__Rewire__('markFeedStale', fakeMarkFeedStale);
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger
            });
            await feedService.updateFeedsToStale([], podId);
            expect(fakeMarkFeedStale).to.have.been.not.called;
        });
    });

    describe('Update feeds to be reuse', () => {
        it('Should update the feeds to be reuse', async () => {
            const localLogger = {
                warn: sinon.fake(),
                debug: sinon.fake(),
            };
            const busService = {
                sendRecycleFeed: sinon.fake.resolves()
            };
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger: localLogger
            });
            const result = await feedService.updateFeedsToReuse(oneFeed, podId);
            expect(result.numFeedUpdatedToReuse).to.equals(1);
            expect(localLogger.warn).to.have.not.been.called;
            expect(localLogger.debug).to.have.been.calledOnce;
        });
        it('Should fail update the feeds to be reuse', async () => {
            const localLogger = {
                warn: sinon.fake(),
                debug: sinon.fake(),
            };
            const busService = {
                sendRecycleFeed: sinon.fake.rejects('Fail to update feed')
            };
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger: localLogger
            });
            const result = await feedService.updateFeedsToReuse(oneFeed, podId);
            expect(result.numFeedUpdatedToReuse).to.equals(0);
            expect(localLogger.warn).to.have.been.calledOnce;
            expect(localLogger.debug).to.have.been.calledOnce;
        });
        it('Should update one and fail another the feeds to be reuse', async () => {
            const localLogger = {
                warn: sinon.fake(),
                debug: sinon.fake(),
            };
            const twoFeeds = [
                ...oneFeed,
                {
                    feedId: '2',
                    userId: 2,
                    feedsKey: 'mt:df:f:u:2'
                },
            ];
            const stubSendRecycleFeed = sinon.stub();
            const busService = {
                sendRecycleFeed: stubSendRecycleFeed
            };
            stubSendRecycleFeed.onCall(0).resolves();
            stubSendRecycleFeed.onCall(1).rejects('Fail to update feed');
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger: localLogger
            });
            const result = await feedService.updateFeedsToReuse(twoFeeds, podId);
            expect(result.numFeedUpdatedToReuse).to.equals(1);
            expect(localLogger.warn).to.have.been.calledOnce;
            expect(localLogger.debug).to.have.been.calledOnce;
        });
        it('Should do nothing when there no feeds to be reuse', async () => {
            const stubSendRecycleFeed = sinon.stub();
            const busService = {
                sendRecycleFeed: stubSendRecycleFeed
            };
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger
            });
            await feedService.updateFeedsToReuse([], podId);
            expect(stubSendRecycleFeed).to.have.been.not.called;
        });
    });

    describe('Recycling feeds', () => {
        const feedsToStale = [
            {
                feedId: '1',
                userId: 1,
                feedsKey: 'mt:df:f:u:1'
            }
        ];
        const feedsToReuse = [
            {
                feedId: '2',
                userId: 2,
                feedsKey: 'mt:df:f:u:2'
            }
        ];
        const feedsToRemove = [
            {
                feedId: '3',
                userId: 3,
                feedsKey: 'mt:df:f:u:3'
            }
        ];

        it('recyclyingFeeds() should update the stale/reuse feeds', async () => {
            const fakeRemoveFeeds = sinon.fake.resolves();
            const fakeSendRecycleFeed = sinon.fake.resolves();
            const fakeMarkFeedStale = sinon.fake.resolves();
            const busService = {
                sendRecycleFeed: fakeSendRecycleFeed
            };
            FeedService.__Rewire__('removeFeeds', fakeRemoveFeeds);
            FeedService.__Rewire__('markFeedStale', fakeMarkFeedStale);
            const feedService = new FeedService({
                sqsClient, ddbDaxClient, ddbDirectClient, options, busService, logger
            });
            await feedService.recyclingFeeds(feedsToStale, feedsToReuse, feedsToRemove, podId);
            expect(fakeMarkFeedStale).to.have.been.calledOnce;
            expect(fakeSendRecycleFeed).to.have.been.calledOnce;
            expect(fakeRemoveFeeds).to.have.been.calledOnce;
        });
    });
});
