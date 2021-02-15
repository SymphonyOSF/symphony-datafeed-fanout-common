import get from 'lodash/get';

const maestroEventPrefix = 'com.symphony.s2.model.chat.MaestroMessage.';
const maestroEventPrefixLength = maestroEventPrefix.length;
const C_JOIN_ROOM_MAESTRO_EVENT = 'JOIN_ROOM';
const allowedMaestroEvents = [
    'INSTANT_CHAT',
    'CREATE_IM',
    'DEACTIVATE_IM',
    'JOIN_ROOM',
    'JOIN_ROOM_REJECTED',
    'ACTIVATE_ROOM',
    'REACTIVATE_ROOM',
    'CREATE_ROOM',
    'LEAVE_ROOM',
    'DEACTIVATE_ROOM',
    'MEMBER_MODIFIED',
    'UPDATE_ROOM',
    'UPDATE_STREAM',
    'IGNORE_ROOM_REQUEST',
    'ROOM_REQUEST',
    'PROMOTE_TO_PERSISTENT',
    'CHANNEL_DELETE',
    'CHANNEL_CREATE',
    'CHANNEL_UPDATE',
    'CHANNEL_SUBSCRIBE',
    'CHANNEL_UNSUBSCRIBE',
    'FILTER_UPDATE',
    'FILTER_DELETE',
    'FILTER_CREATE',
    'CONNECTION_REQUEST_ALERT',
    'MESSAGE_SUPPRESSION',
    'INITIATE_SCREENSHARING',
    'STOP_SCREENSHARING',
    'JOIN_SCREENSHARING',
    'LEAVE_SCREENSHARING',
    'INITIATE_SWITCH_SCREENSHARING',
    'CANCEL_SWITCH_SCREENSHARING',
    'SWITCH_SCREENSHARING',
    'ENABLED_EMAIL_INTEGRATION',
    'DISABLED_EMAIL_INTEGRATION',
    'MALWARE_SCAN_STATE_UPDATE',
    'STREAM_INVITATION'
];

function shouldIgnoreMaestroEvent(message, maestroEvent) {
    if (maestroEvent === C_JOIN_ROOM_MAESTRO_EVENT) {
        return get(message, 'payload.payload.pending', false);
    }
    return false;
}

export default function validatePayloadType(data, payloadType) {
    let success = true;
    let message = '';

    if (payloadType.includes(maestroEventPrefix)) {
        const payloadTypeSize = payloadType.length;
        const maestroEvent = payloadType.substring(payloadTypeSize, maestroEventPrefixLength);
        if (shouldIgnoreMaestroEvent(data, maestroEvent) || !allowedMaestroEvents.includes(maestroEvent)) {
            message = `Invalid message: Event not allowed or should be ignored - Maestro-${maestroEvent}`;
            success = false;
        }
    }
    return { success, message };
}
