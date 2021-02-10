/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/* eslint-disable no-unused-expressions */
/* eslint-disable no-magic-numbers */
import { describe } from 'mocha';
import chai from 'chai';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';
import chaiAsPromised from 'chai-as-promised';

import ObjectStorageService from '../../src/ObjectStorageService';

chai.use(sinonChai);
chai.use(chaiAsPromised);

const expect = chai.expect;

describe('ObjectStorageService Tests', () => {

    const s3 = sinon.stub();
    const s3GetObject = sinon.stub();
    const data = {};

    let objectStorageService;

    beforeEach(() => {
        ObjectStorageService.__Rewire__('s3GetObject', s3GetObject);
        objectStorageService = new ObjectStorageService(s3);
    });

    afterEach(() => {
        s3GetObject.reset();
    });

    describe('Get payload from s3 feeds', () => {

        it('Should get payload with success', async () => {
            s3GetObject.resolves(true);
            const result = await objectStorageService.getPayload(data);
            expect(result).to.be.true;
        });

        it('Should not get payload from s3', async () => {
            s3GetObject.rejects({ message: 'error' });
            try {
                await objectStorageService.getPayload(data);
            } catch (e) {
                expect(e.message).to.eq('error');
            }
        });
    });

});
