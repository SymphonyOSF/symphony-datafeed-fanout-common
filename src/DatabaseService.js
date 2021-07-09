import { feeds as coreFeeds } from 'symphony-datafeed-core';

const { findFeedsByMessage } = coreFeeds;
export default class DatabaseService {

    constructor({
        daxClient, directClient, tableName, staleFeedsTtl
    }) {
        this.daxClient = daxClient;
        this.directClient = directClient;
        this.tableName = tableName;
        this.staleFeedsTtl = staleFeedsTtl;
    }

    fetchFeeds(message) {
        return findFeedsByMessage({
            daxClient: this.daxClient,
            directClient: this.directClient,
            tableName: this.tableName,
            staleFeedsTtl: this.staleFeedsTtl,
            message
        });
    }

}
