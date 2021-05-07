import { s3GetObject } from 'symphony-datafeed-core';

export default class ObjectStorageService {

    constructor(s3Client) {
        this.s3Client = s3Client;
    }

    getPayload(data) {
        const {
            s3BucketName,
            s3Key
        } = data;
        return s3GetObject(this.s3Client, s3BucketName, s3Key);
    }

}
