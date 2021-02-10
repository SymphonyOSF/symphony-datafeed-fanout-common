import findFeeds from 'symphony-datafeed-core/findFeeds';

export default class DatabaseService {

    constructor({ ddbDocumentClient, tableName, staleFeedsTtl }) {
        this.ddbDocumentClient = ddbDocumentClient;
        this.tableName = tableName;
        this.staleFeedsTtl = staleFeedsTtl;
    }

    fetchFeeds(message, listOfIds) {
        return findFeeds(
            this.ddbDocumentClient,
            this.tableName,
            listOfIds,
            this.staleFeedsTtl,
            message
        );
    }

}
