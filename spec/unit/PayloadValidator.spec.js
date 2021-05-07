/* eslint-disable no-undef */
/* eslint-disable no-unused-expressions */
import { describe } from 'mocha';
import { expect } from 'chai';

import validatePayloadType from '../../src/checkers/PayloadValidator';

describe('Payload Validator test', () => {
    it('should return invalid message', () => {
        const maestro = 'com.symphony.s2.model.chat.MaestroMessage.EVENT';
        const resp = validatePayloadType({}, maestro);
        expect(resp.success).to.be.false;
    });
    it('should return success message', () => {
        const maestro = 'com.symphony.s2.model.chat.MaestroMessage.CREATE_IM';
        const resp = validatePayloadType({}, maestro);
        expect(resp.success).to.be.true;
    });
    it('should ignore for Join Room ', () => {
        const maestro = 'com.symphony.s2.model.chat.MaestroMessage.JOIN_ROOM';
        const resp = validatePayloadType({ payload: { payload: { pending: true } } }, maestro);
        expect(resp.success).to.be.false;
    });
    it('should accept stream invitation', () => {
        const maestro = 'com.symphony.s2.model.chat.MaestroMessage.STREAM_INVITATION';
        const resp = validatePayloadType({ payload: { payload: { pending: true } } }, maestro);
        expect(resp.success).to.be.true;
    });
});
