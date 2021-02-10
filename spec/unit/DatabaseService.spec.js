/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/* eslint-disable no-unused-expressions */
/* eslint-disable no-magic-numbers */
import { describe } from 'mocha';
import chai from 'chai';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';
import chaiAsPromised from 'chai-as-promised';

import DatabaseService from '../../src/DatabaseService';

chai.use(sinonChai);
chai.use(chaiAsPromised);

const expect = chai.expect;

describe('DatabaseService Tests', () => {

    const listOfIds = [];
    const ddbDocumentClient = sinon.stub();
    const findFeeds = sinon.stub();
    const message = {};

    let databaseService;

    beforeEach(() => {
        DatabaseService.__Rewire__('findFeeds', findFeeds);
        databaseService = new DatabaseService({ ddbDocumentClient, tableName: 'table-1', staleFeedsTtl: 360 });
    });

    afterEach(() => {
        findFeeds.reset();
    });

    describe('Fetch feeds', () => {

        it('Should fetch feeds with success', async () => {
            findFeeds.resolves(true);
            const result = await databaseService.fetchFeeds(message, listOfIds);
            expect(result).to.be.true;
        });

        it('Should not fetch feeds', async () => {
            findFeeds.rejects({ message: 'error' });
            try {
                await databaseService.fetchFeeds(message, listOfIds);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

});
