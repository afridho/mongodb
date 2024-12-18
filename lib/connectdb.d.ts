/// <reference types="mongodb" />

declare class ClientDB {
    private client: MongoClient;
    private collectionName: string;
    private collection: Collection | null;

    /**
     * Creates an instance of ClientDB.
     * @param {string} collectionName - The name of the collection to interact with.
     */
    constructor(collectionName: string);

    /**
     * Connects to the MongoDB database and initializes the collection.
     * @returns {Promise<void>}
     */
    connect(): Promise<void>;

    /**
     * Reads a document from the collection based on the provided query.
     * @param {Object} query - The query to find the document.
     * @returns {Promise<Object|null>} The found document or null if not found.
     */
    read(query: object): Promise<object | null>;

    /**
     * Reads all documents from the collection.
     * @returns {Promise<Array<Object>>} An array of all documents in the collection.
     */
    readAll(): Promise<object[]>;

    /**
     * Inserts a new document into the collection.
     * @param {Object} data - The data to insert.
     * @returns {Promise<Object>} The result of the insert operation.
     */
    insert(data: object): Promise<object>;

    /**
     * Inserts multiple documents into the collection.
     * @param {Array<Object>} data - The data to insert.
     * @returns {Promise<Object>} The result of the insert operation.
     */
    insertMany(data: object[]): Promise<object>;

    /**
     * Updates a document in the collection based on the provided query.
     * @param {Object} query - The query to find the document to update.
     * @param {Object} data - The data to update.
     * @returns {Promise<Object>} The result of the update operation.
     */
    update(query: object, data: object): Promise<object>;

    /**
     * Deletes a document from the collection based on the provided query.
     * @param {Object} query - The query to find the document to delete.
     * @returns {Promise<Object>} The result of the delete operation.
     */
    delete(query: object): Promise<object>;

    /**
     * Deletes multiple documents from the collection based on the provided query.
     * @param {Object} query - The query to find the documents to delete.
     * @returns {Promise<Object>} The result of the delete operation.
     */
    deleteMany(query: object): Promise<object>;

    /**
     * Finds multiple documents in the collection based on the provided query.
     * @param {Object} query - The query to find the documents.
     * @returns {Promise<Array>} An array of found documents.
     */
    find(query: object): Promise<object[]>;

    /**
     * Gets the storage statistics for the collection.
     * @returns {Promise<Object>} The storage statistics including storageSize.
     */
    getStorageStats(): Promise<{
        storageSize: number;
        size: number;
        count: number;
    }>;

    /**
     * Closes the MongoDB connection.
     * @returns {Promise<void>}
     */
    close(): Promise<void>;
}

export = ClientDB;
