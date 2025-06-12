import {
    MongoClient,
    Collection,
    Document,
    MongoClientOptions,
    ObjectId,
} from "mongodb";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

// Explicitly define the type and provide a default empty string to satisfy TypeScript
const DB_URI: string = process.env.MONGODB_URI || "";
const DB_NAME: string = process.env.DB_NAME || "";

// Validate environment variables before proceeding
if (!DB_URI) {
    throw new Error("MONGODB_URI must be set in the environment");
}

if (!DB_NAME) {
    throw new Error("DB_NAME must be set in the environment");
}

// Define MongoDB client options
const mongoOptions: MongoClientOptions = {
    connectTimeoutMS: 50000,
};

interface ReadAllOptions {
    /**
     * Optional sort object, e.g. { createdAt: 1 } for ascending or { createdAt: -1 } for descending
     */
    sort?: Document;
    limit?: number;
}

/**
 * Class representing a MongoDB client for a specific collection.
 */
class ClientDB {
    private client: MongoClient;
    private collectionName: string;
    private collection: Collection | null;

    /**
     * Creates an instance of ClientDB.
     * @param {string} collectionName - The name of the collection to interact with.
     */
    constructor(collectionName: string) {
        // Use the validated DB_URI and predefined options
        this.client = new MongoClient(DB_URI, mongoOptions);
        this.collectionName = collectionName;
        this.collection = null;
    }

    /**
     * Connects to the MongoDB database and initializes the collection.
     * @returns {Promise<void>}
     */
    async connect(): Promise<void> {
        try {
            if (!this.collection) {
                await this.client.connect();
                const database = this.client.db(DB_NAME);
                this.collection = database.collection(this.collectionName);
            }
        } catch (error) {
            console.error("Connection error:", error);
            throw error;
        }
    }

    /**
     * Helper to convert _id string to ObjectId if needed
     * @param {Document} query - The query object to preprocess
     * @returns {Document} The processed query with _id converted if applicable
     */
    private preprocessQuery(query: Document): Document {
        if (query._id && typeof query._id === "string") {
            try {
                query._id = new ObjectId(query._id);
            } catch {
                // Invalid ObjectId string, leave as is or handle error if you prefer
            }
        }
        return query;
    }

    /**
     * Reads a document from the collection based on the provided query.
     * @param {Document} query - The query to find the document.
     * @returns {Promise<Document|null>} The found document or null if not found.
     */
    async read(query: Document): Promise<Document | null> {
        await this.connect();
        const processedQuery = this.preprocessQuery(query);
        return await this.collection!.findOne(processedQuery);
    }

    /**
     * Reads all documents from the collection with optional sorting.
     * @param {ReadAllOptions} [options] - Optional options object.
     * @param {Document} [options.sort] - Optional sort object, e.g. { createdAt: 1 } for ascending.
     * @returns {Promise<Document[]>} An array of all documents, optionally sorted.
     */
    async readAll(options?: ReadAllOptions): Promise<Document[]> {
        await this.connect();

        let cursor = this.collection!.find();

        if (options?.sort) {
            cursor = cursor.sort(options.sort);
        }
        if (options?.limit) {
            cursor = cursor.limit(options.limit);
        }

        return await cursor.toArray();
    }

    /**
     * Inserts a new document into the collection.
     * @param {Document} data - The data to insert.
     * @returns {Promise<Document>} The result of the insert operation.
     */
    async insert(data: Document): Promise<Document> {
        await this.connect();
        return await this.collection!.insertOne(data);
    }

    /**
     * Inserts multiple documents into the collection.
     * @param {Document[]} data - The data to insert.
     * @returns {Promise<Document>} The result of the insert operation.
     */
    async insertMany(data: Document[]): Promise<Document> {
        await this.connect();
        return await this.collection!.insertMany(data);
    }

    /**
     * Updates a document in the collection based on the provided query.
     * @param {Document} query - The query to find the document to update.
     * @param {Document} data - The data to update.
     * @returns {Promise<Document>} The result of the update operation.
     */
    async update(query: Document, data: Document): Promise<Document> {
        await this.connect();
        const processedQuery = this.preprocessQuery(query);
        return await this.collection!.updateOne(processedQuery, { $set: data });
    }

    /**
     * Deletes a document from the collection based on the provided query.
     * @param {Document} query - The query to find the document to delete.
     * @returns {Promise<Document>} The result of the delete operation.
     */
    async delete(query: Document): Promise<Document> {
        await this.connect();
        const processedQuery = this.preprocessQuery(query);
        return await this.collection!.deleteOne(processedQuery);
    }

    /**
     * Deletes multiple documents from the collection based on the provided query.
     * @param {Document} query - The query to find the documents to delete.
     * @returns {Promise<Document>} The result of the delete operation.
     */
    async deleteMany(query: Document): Promise<Document> {
        await this.connect();
        const processedQuery = this.preprocessQuery(query);
        return await this.collection!.deleteMany(processedQuery);
    }

    /**
     * Finds multiple documents in the collection based on the provided query.
     * @param {Document} query - The query to find the documents.
     * @returns {Promise<Document[]>} An array of found documents.
     */
    async find(query: Document): Promise<Document[]> {
        await this.connect();
        const processedQuery = this.preprocessQuery(query);
        return await this.collection!.find(processedQuery).toArray();
    }

    /**
     * Gets random documents from the collection.
     * @param {number} [total=1] - The number of random documents to retrieve.
     * @returns {Promise<Document[]>} An array of random documents.
     */
    async getRandomData(total: number = 1): Promise<Document[]> {
        await this.connect();
        const pipeline = [{ $sample: { size: total } }];
        return await this.collection!.aggregate(pipeline).toArray();
    }

    /**
     * Gets the storage statistics for the collection.
     * @returns {Promise<{storageSize: number, size: number, count: number}>} The storage statistics including storageSize.
     */
    async getStorageStats(): Promise<{
        storageSize: number;
        size: number;
        count: number;
    }> {
        await this.connect();
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
    async close(): Promise<void> {
        await this.client.close();
    }
}

export default ClientDB;
