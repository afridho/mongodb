require("dotenv").config();
const DB_URI = process.env.MONGODB_URI;
const DB_NAME = process.env.DB_NAME;
const { MongoClient } = require("mongodb");

/**
 * Class representing a MongoDB client for a specific collection.
 */
class ClientDB {
    /**
     * Creates an instance of ClientDB.
     * @param {string} collectionName - The name of the collection to interact with.
     */
    constructor(collectionName) {
        this.client = new MongoClient(DB_URI, { connectTimeoutMS: 50000 });
        this.collectionName = collectionName;
        this.collection = null;
    }

    /**
     * Connects to the MongoDB database and initializes the collection.
     * @returns {Promise<void>}
     */
    async connect() {
        try {
            if (!this.collection) {
                await this.client.connect();
                const database = this.client.db(DB_NAME);
                this.collection = database.collection(this.collectionName);
            }
        } catch (error) {
            throw error;
        }
    }

    /**
     * Reads a document from the collection based on the provided query.
     * @param {Object} query - The query to find the document.
     * @example
     * const result = await read({ label: 'label name' });
     * @returns {Promise<Object|null>} The found document or null if not found.
     */
    async read(query) {
        await this.connect(); // Ensure the connection is established
        return await this.collection.findOne(query);
    }

    /**
     * Reads all documents from the collection.
     * @returns {Promise<Array<Object>>} An array of all documents in the collection.
     */
    async readAll() {
        await this.connect(); // Ensure the connection is established
        return await this.collection.find().toArray();
    }

    /**
     * Inserts a new document into the collection.
     * @param {Object} data - The data to insert.
     * @example
     * await insert({ name: 'New App', version: '1.0.0' });
     * @returns {Promise<Object>} The result of the insert operation.
     */
    async insert(data) {
        await this.connect(); // Ensure the connection is established
        return await this.collection.insertOne(data);
    }

    /**
     * Inserts multiple documents into the collection.
     * @param {Array<Object>} data - The data to insert.
     * @example
     * await insertMany([{ name: 'New App 1', version: '1.0.0' }, { name: 'New App 2', version: '1.0.1' }]);
     * @returns {Promise<Object>} The result of the insert operation.
     */
    async insertMany(data) {
        await this.connect(); // Ensure the connection is established
        return await this.collection.insertMany(data);
    }

    /**
     * Updates a document in the collection based on the provided query.
     * @param {Object} query - The query to find the document to update.
     * @param {Object} data - The data to update.
     * @example
     * await update({ label: 'label name' }, { version: '1.0.1' });
     * @returns {Promise<Object>} The result of the update operation.
     */
    async update(query, data) {
        await this.connect(); // Ensure the connection is established
        return await this.collection.updateOne(query, { $set: data });
    }

    /**
     * Deletes a document from the collection based on the provided query.
     * @param {Object} query - The query to find the document to delete.
     * @example
     * await delete({ label: 'label name' });
     * @returns {Promise<Object>} The result of the delete operation.
     */
    async delete(query) {
        await this.connect(); // Ensure the connection is established
        return await this.collection.deleteOne(query);
    }

    /**
     * Deletes multiple documents from the collection based on the provided query.
     * @param {Object} query - The query to find the documents to delete.
     * @example
     * await deleteMany({ label: 'label name' });
     * @returns {Promise<Object>} The result of the delete operation.
     */
    async deleteMany(query) {
        await this.connect(); // Ensure the connection is established
        return await this.collection.deleteMany(query);
    }

    /**
     * Finds multiple documents in the collection based on the provided query.
     * @param {Object} query - The query to find the documents.
     * @example
     * const results = await find({ label: 'label name' });
     * @returns {Promise<Array>} An array of found documents.
     */
    async find(query) {
        await this.connect(); // Ensure the connection is established
        return await this.collection.find(query).toArray();
    }

    /**
     * Gets the storage statistics for the collection.
     * @returns {Promise<Object>} The storage statistics including storageSize.
     */
    async getStorageStats() {
        await this.connect(); // Ensure the connection is established
        const stats = await this.client
            .db(DB_NAME)
            .command({ collStats: this.collectionName });
        return {
            storageSize: stats.storageSize,
            size: stats.size,
            count: stats.count,
        };
    }

    /**
     * Closes the MongoDB connection.
     * @returns {Promise<void>}
     */
    async close() {
        await this.client.close();
    }
}

module.exports = ClientDB;
