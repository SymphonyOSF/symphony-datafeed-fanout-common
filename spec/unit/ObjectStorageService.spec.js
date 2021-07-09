/* eslint-disable no-unused-expressions */
/* eslint-disable no-undef */
import { describe } from 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import ObjectStorageService from '../../src/ObjectStorageService';

describe('ObjectStorageService Tests', () => {
    afterEach(() => {
        sinon.reset();
        sinon.restore();
        __rewire_reset_all__();
    });

    describe('Get payload from s3 feeds', () => {
        it('Should get payload with success', async () => {
            const fake = sinon.fake.resolves(true);
            ObjectStorageService.__Rewire__('getIngestedMessageFromStorage', fake);
            const objectStorageService = new ObjectStorageService(null);
            const result = await objectStorageService.getPayload({});
            expect(fake).to.be.calledOnceWith({
                s3Client: null,
                bucket: undefined,
                key: undefined,
            });
            expect(result).to.be.true;
        });
        it('Should not get payload from s3', async () => {
            ObjectStorageService.__Rewire__('getIngestedMessageFromStorage', sinon.fake.rejects(new Error('42')));
            const objectStorageService = new ObjectStorageService(null);
            let result;
            try {
                result = await objectStorageService.getPayload({});
            } catch (error) {
                expect(error.message).to.equals('42');
            }
            expect(result).to.be.undefined;
        });
    });
});
