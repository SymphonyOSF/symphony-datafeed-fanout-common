import { findFeeds } from 'symphony-datafeed-core';

export default class DatabaseService {

    constructor({
        daxClient, directClient, tableName, staleFeedsTtl
    }) {
        this.daxClient = daxClient;
        this.directClient = directClient;
        this.tableName = tableName;
        this.staleFeedsTtl = staleFeedsTtl;
    }

    fetchFeeds(message, listOfIds) {
        return findFeeds({
            daxClient: this.daxClient,
            directClient: this.directClient,
            tableName: this.tableName,
            staleFeedsTtl: this.staleFeedsTtl,
            ids: listOfIds,
            message
        });
    }

}
