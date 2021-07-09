import { feeds as coreFeeds } from 'symphony-datafeed-core';

const { getIngestedMessageFromStorage } = coreFeeds.consumers;
export default class ObjectStorageService {

    constructor(s3Client) {
        this.s3Client = s3Client;
    }

    getPayload(data) {
        const {
            s3BucketName: bucket,
            s3Key: key
        } = data;
        return getIngestedMessageFromStorage({
            s3Client: this.s3Client,
            bucket,
            key
        });
    }

}
