/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/* eslint-disable no-unused-expressions */
/* eslint-disable no-magic-numbers */
import { describe } from 'mocha';
import chai from 'chai';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';
import chaiAsPromised from 'chai-as-promised';

import FeedService from '../../src/FeedService';

chai.use(sinonChai);
chai.use(chaiAsPromised);

const expect = chai.expect;

describe('FeedService Tests', () => {

    const podId = 123;
    const ddbDocumentClient = sinon.stub();
    const sqsClient = sinon.stub();
    const removeFeeds = sinon.stub();
    const options = {};
    const busService = {
        sendRecycleFeed: sinon.stub()
    };
    const updateDdbFields = sinon.stub();

    const logger = {
        debug: () => { },
        info: () => { },
        warn: () => { },
        error: () => { },
    };

    let feedService;

    beforeEach(() => {
        FeedService.__Rewire__('removeFeeds', removeFeeds);
        FeedService.__Rewire__('updateDdbFields', updateDdbFields);
        feedService = new FeedService({
            sqsClient, ddbDocumentClient, options, busService, logger
        });
    });

    afterEach(() => {
        removeFeeds.reset();
        busService.sendRecycleFeed.reset();
        updateDdbFields.reset();
    });

    describe('Remove invalid feeds', () => {

        const feedsToBeDeleted = [
            {
                feedId: '1',
                userId: 1,
                feedsKey: 'mt:df:f:u:1'
            }
        ];
        it('Should remove invalid feeds with success', async () => {
            const expected = ([ [ Promise.resolve() ], Promise.resolve() ]);
            removeFeeds.resolves(expected);
            const result = await feedService.removeFeeds(feedsToBeDeleted, podId);
            expect(result).to.be.equal(expected);
        });

        it('Remove feeds should return an rejection', async () => {
            removeFeeds.rejects({ message: 'error' });
            try {
                await feedService.removeFeeds(feedsToBeDeleted, podId);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

    describe('Update feeds to be stale', () => {

        it('Should update the feeds to be stale', async () => {
            const feedsToBeStale = [
                {
                    feedId: '1',
                    userId: 1,
                    feedsKey: 'mt:df:f:u:1'
                }
            ];
            updateDdbFields.resolves();
            const result = await feedService.updateFeedsToStale(feedsToBeStale, podId);
            expect(result.numFeedUpdatedToStale).to.be.eq(1);
        });

        it('Should fail update the feeds to be stale', async () => {
            const feedsToBeStale = [
                {
                    feedId: '1',
                    userId: 1,
                    feedsKey: 'mt:df:f:u:1'
                }
            ];
            updateDdbFields.rejects('Fail to update');
            const result = await feedService.updateFeedsToStale(feedsToBeStale, podId);
            expect(result.numFeedUpdatedToStale).to.be.eq(0);
        });

        it('Should update one and fail another the feeds to be stale', async () => {
            const feedsToBeStale = [
                {
                    feedId: '1',
                    userId: 1,
                    feedsKey: 'mt:df:f:u:1'
                },
                {
                    feedId: '2',
                    userId: 2,
                    feedsKey: 'mt:df:f:u:2'
                },
            ];
            updateDdbFields.onCall(0).resolves();
            updateDdbFields.onCall(1).rejects('Fail to update feed');
            const result = await feedService.updateFeedsToStale(feedsToBeStale, podId);
            expect(result.numFeedUpdatedToStale).to.be.eq(1);
        });

        it('Should do nothing when there no feeds', async () => {
            const feedsToBeStale = [];
            await feedService.updateFeedsToStale(feedsToBeStale, podId);
            expect(updateDdbFields).to.have.been.not.called;
        });
    });

    describe('Update feeds to be reuse', () => {

        it('Should update the feeds to be reuse', async () => {
            const feedsToBeReuse = [
                {
                    feedId: '1',
                    userId: 1,
                    feedsKey: 'mt:df:f:u:1'
                }
            ];
            busService.sendRecycleFeed.resolves();
            const result = await feedService.updateFeedsToReuse(feedsToBeReuse, podId);
            expect(result.numFeedUpdatedToReuse).to.be.eq(1);
        });

        it('Should fail update the feeds to be reuse', async () => {
            const feedsToBeReuse = [
                {
                    feedId: '1',
                    userId: 1,
                    feedsKey: 'mt:df:f:u:1'
                }
            ];
            busService.sendRecycleFeed.rejects('Fail to update feed');
            const result = await feedService.updateFeedsToReuse(feedsToBeReuse, podId);
            expect(result.numFeedUpdatedToReuse).to.be.eq(0);
        });

        it('Should update one and fail another the feeds to be reuse', async () => {
            const feedsToBeReuse = [
                {
                    feedId: '1',
                    userId: 1,
                    feedsKey: 'mt:df:f:u:1'
                },
                {
                    feedId: '2',
                    userId: 2,
                    feedsKey: 'mt:df:f:u:2'
                },
            ];
            busService.sendRecycleFeed.onCall(0).resolves();
            busService.sendRecycleFeed.onCall(1).rejects('Fail to update feed');
            const result = await feedService.updateFeedsToReuse(feedsToBeReuse, podId);
            expect(result.numFeedUpdatedToReuse).to.be.eq(1);
        });

        it('Should do nothing when there no feeds to be reuse', async () => {
            const feedsToBeReuse = [];
            await feedService.updateFeedsToReuse(feedsToBeReuse, podId);
            expect(busService.sendRecycleFeed).to.have.been.not.called;
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

        beforeEach(async ()=> {
            removeFeeds.resolves();
            busService.sendRecycleFeed.resolves();
            updateDdbFields.resolves();
            await feedService.recyclingFeeds(feedsToStale, feedsToReuse, feedsToRemove, podId);
        });

        it('should update the stale feeds', () => {
            expect(updateDdbFields).to.have.been.calledOnce;
        });

        it('should update the resue feeds', () => {
            expect(busService.sendRecycleFeed).to.have.been.calledOnce;
        });

        it('should update the stale feeds', () => {
            expect(removeFeeds).to.have.been.calledOnce;
        });
    });
});
