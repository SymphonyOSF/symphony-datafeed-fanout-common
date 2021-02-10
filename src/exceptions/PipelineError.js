export default class PipelineError extends Error {

    constructor(params) {
        const {
            message,
            discardOriginalMessage
        } = params;
        super(message);
        this.discardOriginalMessage = discardOriginalMessage;
    }

}
