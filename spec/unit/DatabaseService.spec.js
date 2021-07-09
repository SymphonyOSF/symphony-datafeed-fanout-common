/* eslint-disable no-unused-expressions */
/* eslint-disable no-undef */
import { describe } from 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import DatabaseService from '../../src/DatabaseService';

describe('DatabaseService Tests', () => {
    afterEach(() => {
        sinon.reset();
        sinon.restore();
        __rewire_reset_all__();
    });

    describe('Fetch feeds', () => {
        it('Should fetch feeds with success', async () => {
            const fake = sinon.fake.resolves(true);
            DatabaseService.__Rewire__('findFeedsByMessage', fake);
            const databaseService = new DatabaseService({
                daxClient: null, directClient: null, tableName: 'table', staleFeedsTtl: {}
            });
            const result = await databaseService.fetchFeeds({});
            expect(fake).to.be.calledOnceWith({
                daxClient: null,
                directClient: null,
                tableName: 'table',
                staleFeedsTtl: {},
                message: {},
            });
            expect(result).to.be.true;
        });
        it('Should not fetch feeds', async () => {
            DatabaseService.__Rewire__('findFeedsByMessage', sinon.fake.rejects(new Error('42')));
            const databaseService = new DatabaseService({
                daxClient: null, directClient: null, tableName: 'table', staleFeedsTtl: {}
            });
            let result;
            try {
                result = await databaseService.fetchFeeds({}, []);
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
    });

});
