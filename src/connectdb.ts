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
    skip?: number;
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
        if (options?.skip) {
            cursor = cursor.skip(options.skip);
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
     * Updates multiple documents in the collection.
     * - If you pass Mongo operators ($set, $inc, etc), it will use them directly.
     * - If you pass a plain object, it will wrap it inside $set.
     *
     * @param {Document} query - The filter to match documents.
     * @param {Document} data - The update data (plain object or with operators).
     * @returns {Promise<Document>} The result of the update operation.
     */
    async updateMany(
        query: Document,
        data: Document | Document[]
    ): Promise<Document> {
        await this.connect();
        const processedQuery = this.preprocessQuery(query);

        if (Array.isArray(data)) {
            return await this.collection!.updateMany(processedQuery, data);
        }

        const hasOperator = Object.keys(data).some((key) =>
            key.startsWith("$")
        );
        const updateDoc = hasOperator ? data : { $set: data };

        return await this.collection!.updateMany(processedQuery, updateDoc);
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
    async find(
        query: Document,
        options?: ReadAllOptions,
        project?: Document
    ): Promise<Document[]> {
        await this.connect();
        const processedQuery = this.preprocessQuery(query);

        let cursor = this.collection!.find(
            processedQuery,
            project ? { projection: project } : undefined
        );

        if (options?.sort) {
            cursor = cursor.sort(options.sort);
        }
        if (options?.limit) {
            cursor = cursor.limit(options.limit);
        }
        if (options?.skip) {
            cursor = cursor.skip(options.skip);
        }

        return await cursor.toArray();
    }

    /**
     * Reads multiple documents from the collection based on a query.
     * @param {Document} query - The filter query.
     * @returns {Promise<Document[]>} Array of matching documents.
     */
    async readMany(query: Document): Promise<Document[]> {
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
     * Run an aggregation pipeline on the current collection.
     *
     * @param {Array<Document>} pipeline - An array of MongoDB aggregation stages.
     *   Example:
     *   [
     *     { $unwind: "$tags" },
     *     { $group: { _id: "$tags", count: { $sum: 1 } } },
     *     { $sort: { count: -1 } },
     *     { $limit: 10 }
     *   ]
     *
     * @returns {Promise<Document[]>} Resolves with the array of aggregation results.
     *
     * @throws {Error} If the aggregation query fails.
     */
    async aggregate(pipeline: Document[] = []): Promise<Document[]> {
        try {
            await this.connect(); // ensure connected
            return await this.collection!.aggregate(pipeline).toArray();
        } catch (err) {
            console.error("Aggregate error:", err);
            throw err;
        }
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

    /**
     * Finds documents in the current collection and dynamically joins related collections
     * using MongoDB's `$lookup` aggregation stage.
     *
     * This is useful when you want to "populate" data from other collections
     * (similar to Mongoose's populate) but in a flexible, dynamic way.
     *
     * ### Example:
     * ```ts
     * const tasks = await tasksDb.findWithRelations(
     *   { column: "todo" }, // filter
     *   [
     *     {
     *       from: "kanban_tags",
     *       localField: "tags",
     *       foreignField: "value",
     *       as: "tags"
     *     },
     *     {
     *       from: "kanban_persons",
     *       localField: "persons",
     *       foreignField: "_id",
     *       as: "persons",
     *       isObjectId: true // convert string IDs to ObjectId
     *     }
     *   ],
     *   {
     *     title: 1,
     *     description: 1,
     *     "tags.label": 1,
     *     "persons.name": 1
     *   },
     *   {
     *     sort: { createdAt: -1 },
     *     limit: 10,
     *     skip: 20
     *   }
     * );
     * ```
     *
     * @param {Document} [filter={}] - MongoDB query filter. Defaults to `{}` (fetch all).
     * @param {Object[]} [relations=[]] - Array of relation configurations for `$lookup`.
     * @param {string} relations[].from - Target collection name to join with.
     * @param {string} relations[].localField - Field in this collection that holds the reference.
     * @param {string} relations[].foreignField - Field in the target collection to match against.
     * @param {string} relations[].as - The alias name for the joined data in the output.
     * @param {boolean} [relations[].isObjectId] - If `true`, will map string IDs in `localField` into ObjectId before lookup.
     * @param {Document} [project={}] - Optional MongoDB projection object to limit fields in the final output.
     * @param {ReadAllOptions} [options={}] - Optional settings like sort, skip, and limit.
     *
     * @returns {Promise<Document[]>} A promise that resolves to an array of documents with joined relations applied.
     */
    async findWithRelations(
        filter: Document = {},
        relations: {
            from: string;
            localField: string;
            foreignField: string;
            as: string;
            isObjectId?: boolean;
            isSingle?: boolean;
        }[] = [],
        project: Document = {},
        options: ReadAllOptions & {
            paginated?: boolean;
            response?: boolean;
            req?: any;
        } = {}
    ): Promise<
        | Document[]
        | {
              success: boolean;
              message: string;
              page: number;
              per_page: number;
              total: number;
              total_pages: number;
              data: Document[];
          }
    > {
        await this.connect();
        const processedQuery = this.preprocessQuery(filter);

        const page = Math.max(parseInt(options.req?.query?.page, 10) || 1, 1);
        const per_page = Math.max(
            parseInt(options.req?.query?.per_page, 10) || 10,
            1
        );
        const skip = (page - 1) * per_page;

        // Base aggregation pipeline
        const pipeline: Document[] = [];

        if (Object.keys(processedQuery).length > 0) {
            pipeline.push({ $match: processedQuery });
        }

        for (const rel of relations) {
            if (rel.isObjectId) {
                if (rel.isSingle) {
                    pipeline.push({
                        $addFields: {
                            [rel.localField]: {
                                $toObjectId: `$${rel.localField}`,
                            },
                        },
                    });
                } else {
                    pipeline.push({
                        $addFields: {
                            [rel.localField]: {
                                $map: {
                                    input: `$${rel.localField}`,
                                    as: "id",
                                    in: { $toObjectId: "$$id" },
                                },
                            },
                        },
                    });
                }
            }

            pipeline.push({
                $lookup: {
                    from: rel.from,
                    localField: rel.localField,
                    foreignField: rel.foreignField,
                    as: rel.as,
                },
            });
        }

        if (options.sort) pipeline.push({ $sort: options.sort });

        // ✅ Pagination behavior
        if (options.paginated) {
            const total = await this.count(processedQuery);
            const total_pages = Math.ceil(total / per_page);
            pipeline.push({ $skip: skip });
            pipeline.push({ $limit: per_page });
            if (Object.keys(project).length > 0)
                pipeline.push({ $project: project });

            const data = await this.collection!.aggregate(pipeline).toArray();

            if (options.response) {
                return {
                    success: true,
                    message: "Success",
                    page,
                    per_page,
                    total,
                    total_pages,
                    data,
                };
            }
            return data;
        }

        // Normal (non-paginated) case
        if (options.skip) pipeline.push({ $skip: options.skip });
        if (options.limit) pipeline.push({ $limit: options.limit });
        if (Object.keys(project).length > 0)
            pipeline.push({ $project: project });

        return await this.collection!.aggregate(pipeline).toArray();
    }

    /**
     * Finds a single document in the current collection and dynamically joins related collections
     * using MongoDB's `$lookup` aggregation stage.
     *
     * Mirip dengan `findWithRelations`, tapi hanya return satu dokumen (bukan array).
     *
     * ### Example:
     * ```ts
     * const task = await tasksDb.findOneWithRelations(
     *   { _id: "66cfa89f3c9c7d776b5f4f10" },
     *   [
     *     {
     *       from: "kanban_tags",
     *       localField: "tags",
     *       foreignField: "value",
     *       as: "tags"
     *     },
     *     {
     *       from: "kanban_persons",
     *       localField: "persons",
     *       foreignField: "_id",
     *       as: "persons",
     *       isObjectId: true
     *     }
     *   ]
     * );
     * ```
     *
     * @param {Document} filter - MongoDB query filter. Biasanya pakai `_id`.
     * @param {Object[]} [relations=[]] - Array of relation configs sama seperti `findWithRelations`.
     * @param {Document} [project={}] - Projection untuk limit field output.
     *
     * @returns {Promise<Document | null>} A single document with joined relations, or `null`.
     */
    async findOneWithRelations(
        filter: Document,
        relations: {
            from: string;
            localField: string;
            foreignField: string;
            as: string;
            isObjectId?: boolean;
            isSingle?: boolean;
        }[] = [],
        project: Document = {}
    ): Promise<Document | null> {
        await this.connect();
        const processedQuery = this.preprocessQuery(filter);

        const pipeline: Document[] = [{ $match: processedQuery }];

        // relations
        for (const rel of relations) {
            if (rel.isObjectId) {
                if (rel.isSingle) {
                    pipeline.push({
                        $addFields: {
                            [rel.localField]: {
                                $toObjectId: `$${rel.localField}`,
                            },
                        },
                    });
                } else {
                    pipeline.push({
                        $addFields: {
                            [rel.localField]: {
                                $map: {
                                    input: `$${rel.localField}`,
                                    as: "id",
                                    in: { $toObjectId: "$$id" },
                                },
                            },
                        },
                    });
                }
            }

            pipeline.push({
                $lookup: {
                    from: rel.from,
                    localField: rel.localField,
                    foreignField: rel.foreignField,
                    as: rel.as,
                },
            });
        }

        if (Object.keys(project).length > 0) {
            pipeline.push({ $project: project });
        }

        const results = await this.collection!.aggregate(pipeline).toArray();
        return results[0] || null;
    }

    /**
     * Counts the number of documents matching the query.
     * Alias for countDocuments.
     * @param {Document} [query={}] - Optional filter query.
     * @returns {Promise<number>} The count of matching documents.
     */
    async count(query: Document = {}): Promise<number> {
        return this.countDocuments(query);
    }

    /**
     * Counts the number of documents matching the query.
     * @param {Document} [query={}] - Optional filter query.
     * @returns {Promise<number>} The count of matching documents.
     */
    async countDocuments(query: Document = {}): Promise<number> {
        await this.connect();
        const processedQuery = this.preprocessQuery(query);
        return await this.collection!.countDocuments(processedQuery);
    }

    /**
     * One-time migration: convert string date fields to Date objects.
     *
     * @param fields - Which fields to convert (default: createdAt, updatedAt, startAt, endAt)
     * @returns number of documents updated
     */
    async migrateDateFields(
        fields: string[] = ["createdAt", "updatedAt", "startAt", "endAt"]
    ): Promise<number> {
        await this.connect();

        const cursor = this.collection!.find({
            $or: fields.map((f) => ({ [f]: { $type: "string" } })),
        });

        let count = 0;
        while (await cursor.hasNext()) {
            const doc = await cursor.next();
            if (!doc) continue; // TS happy + runtime safe

            const updates: Record<string, any> = {};
            for (const field of fields) {
                if (typeof doc[field] === "string") {
                    updates[field] = new Date(doc[field]);
                }
            }

            if (Object.keys(updates).length > 0) {
                await this.collection!.updateOne(
                    { _id: doc._id },
                    { $set: updates }
                );
                count++;
            }
        }

        return count;
    }
}

export default ClientDB;
export { ObjectId };
