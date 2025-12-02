import {
    MongoClient,
    Collection,
    Document,
    MongoClientOptions,
    ObjectId,
} from "mongodb";
import dotenv from "dotenv";
import { getISOWeek, getISOWeekYear, subWeeks } from "date-fns";

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
    connectTimeoutMS: 30000,
    maxPoolSize: 20, // prevent excessive parallel connections
    maxIdleTimeMS: 60000,
};

// --- Global connection cache (important for serverless) ---
declare global {
    // eslint-disable-next-line no-var
    var _mongoClient: MongoClient | undefined;
}

async function getMongoClient(): Promise<MongoClient> {
    if (global._mongoClient) return global._mongoClient;

    console.log("🌱 Connecting to MongoDB...");
    const client = new MongoClient(DB_URI, mongoOptions);
    await client.connect();
    global._mongoClient = client;
    return client;
}

// --- Main class ---
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
    private client!: MongoClient;
    private collectionName: string;
    private collection: Collection | null;
    /**
     * Creates an instance of ClientDB.
     * @param {string} collectionName - The name of the collection to interact with.
     */
    constructor(collectionName: string) {
        // Use the validated DB_URI and predefined options
        this.collectionName = collectionName;
        this.collection = null;
    }

    /**
     * Connects to the MongoDB database and initializes the collection.
     * @returns {Promise<void>}
     */
    async connect(): Promise<void> {
        if (!this.collection) {
            this.client = await getMongoClient();
            const db = this.client.db(DB_NAME);
            this.collection = db.collection(this.collectionName);
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
        data: Document | Document[],
        options: { upsert?: boolean } = {}
    ): Promise<Document> {
        await this.connect();
        const processedQuery = this.preprocessQuery(query);

        const hasOperator =
            !Array.isArray(data) &&
            Object.keys(data).some((key) => key.startsWith("$"));
        const updateDoc = Array.isArray(data)
            ? data
            : hasOperator
            ? data
            : { $set: data };

        return await this.collection!.updateMany(processedQuery, updateDoc, {
            upsert: options.upsert ?? false,
        });
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
            isSingle?: boolean; // 👈 NEW
        }[] = [],
        project: Document = {},
        options?: ReadAllOptions
    ): Promise<Document[]> {
        await this.connect();
        const processedQuery = this.preprocessQuery(filter);

        const pipeline: Document[] = [];

        if (Object.keys(processedQuery).length > 0) {
            pipeline.push({ $match: processedQuery });
        }

        for (const rel of relations) {
            if (rel.isObjectId) {
                if (rel.isSingle) {
                    // 👇 Single string → ObjectId
                    pipeline.push({
                        $addFields: {
                            [rel.localField]: {
                                $toObjectId: `$${rel.localField}`,
                            },
                        },
                    });
                } else {
                    // 👇 Array of strings → map to ObjectIds
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

        if (options?.sort) pipeline.push({ $sort: options.sort });
        if (options?.skip) pipeline.push({ $skip: options.skip });
        if (options?.limit) pipeline.push({ $limit: options.limit });

        if (Object.keys(project).length > 0) {
            pipeline.push({ $project: project });
        }

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
    /**
     * INTERNAL: Connect to external cluster
     */
    private static async _connectExternal(uri: string) {
        const { MongoClient } = require("mongodb");
        const client = new MongoClient(uri, {
            maxPoolSize: 20,
            connectTimeoutMS: 30000,
        });
        await client.connect();
        return client;
    }

    /**
     * INCREMENTAL BACKUP (NO DELETE):
     * - Hanya ambil dokumen yang updatedAt > lastBackupAt
     * - Tidak pernah hapus dokumen di backup
     * - Upsert-only
     */
    static async incrementalBackupOneDatabase(params: {
        targetUri: string;
        sourceDb: string;
        targetDb: string;
    }) {
        const { targetUri, sourceDb, targetDb } = params;

        const sourceClient = await ClientDB._connectExternal(
            process.env.MONGODB_URI!
        );
        const targetClient = await ClientDB._connectExternal(targetUri);

        const src = sourceClient.db(sourceDb);
        const tgt = targetClient.db(targetDb);

        const metaCol = tgt.collection("_backup_meta");
        const meta = await metaCol.findOne({ _id: "incremental" });

        const lastBackupAt = meta?.lastBackupAt
            ? new Date(meta.lastBackupAt)
            : new Date(0);

        const now = new Date();
        const collections = await src.listCollections().toArray();
        const report: any[] = [];

        for (const c of collections) {
            const name = c.name;
            if (name === "_backup_meta") continue;

            const srcCol = src.collection(name);
            const tgtCol = tgt.collection(name);

            // ambil dokumen yang berubah
            const changedDocs = await srcCol
                .find({ updatedAt: { $gt: lastBackupAt } })
                .toArray();

            for (const doc of changedDocs) {
                await tgtCol.updateOne(
                    { _id: doc._id },
                    { $set: doc },
                    { upsert: true }
                );
            }

            report.push({
                collection: name,
                upserted: changedDocs.length,
            });
        }

        // update checkpoint
        await metaCol.updateOne(
            { _id: "incremental" },
            { $set: { lastBackupAt: now } },
            { upsert: true }
        );

        await sourceClient.close();
        await targetClient.close();

        return {
            mode: "incremental",
            sourceDb,
            targetDb,
            lastBackupAt,
            executedAt: now,
            report,
        };
    }

    /**
     * MULTI-DATABASE INCREMENTAL (NO DELETE)
     */
    static async incrementalBackupManyDatabases(
        targetUri: string,
        dbList: string[]
    ) {
        const allResults: any[] = [];

        for (const dbName of dbList) {
            const r = await ClientDB.incrementalBackupOneDatabase({
                targetUri,
                sourceDb: dbName,
                targetDb: dbName,
            });
            allResults.push(r);
        }

        return {
            mode: "incremental",
            targetUri,
            results: allResults,
        };
    }

    /**
     * DELTA BACKUP (NO DELETE):
     * - hanya dokumen baru (yang belum ada _id nya di backup)
     * - tidak pernah hapus
     */
    static async deltaBackupOneDatabase(params: {
        targetUri: string;
        sourceDb: string;
        targetDb: string;
    }) {
        const { targetUri, sourceDb, targetDb } = params;

        const sourceClient = await ClientDB._connectExternal(
            process.env.MONGODB_URI!
        );
        const targetClient = await ClientDB._connectExternal(targetUri);

        const src = sourceClient.db(sourceDb);
        const tgt = targetClient.db(targetDb);

        const collections = await src.listCollections().toArray();
        const report: any[] = [];

        for (const c of collections) {
            const name = c.name;
            const srcCol = src.collection(name);
            const tgtCol = tgt.collection(name);

            // semua _id di backup
            const tgtIds: Set<string> = new Set(
                (
                    await tgtCol.find({}, { projection: { _id: 1 } }).toArray()
                ).map((d: Document) => String(d._id))
            );

            // dokumen yang belum ada
            const newDocs = await srcCol
                .find({
                    _id: {
                        $nin: Array.from(tgtIds).map((id) => new ObjectId(id)),
                    },
                })
                .toArray();

            if (newDocs.length) {
                await tgtCol.insertMany(newDocs);
            }

            report.push({
                collection: name,
                inserted: newDocs.length,
            });
        }

        await sourceClient.close();
        await targetClient.close();

        return {
            mode: "delta",
            sourceDb,
            targetDb,
            report,
        };
    }

    /**
     * MULTI-DATABASE DELTA (NO DELETE)
     */
    static async deltaBackupManyDatabases(targetUri: string, dbList: string[]) {
        const allResults: any[] = [];

        for (const dbName of dbList) {
            const r = await ClientDB.deltaBackupOneDatabase({
                targetUri,
                sourceDb: dbName,
                targetDb: dbName,
            });
            allResults.push(r);
        }

        return {
            mode: "delta",
            targetUri,
            results: allResults,
        };
    }
    static async fullSyncOneDatabase(params: {
        sourceUri: string;
        targetUri: string;
        dbName: string;
        keepWeeks?: number;
    }) {
        const { sourceUri, targetUri, dbName } = params;

        const now = new Date();
        const week = getISOWeek(now);
        const year = getISOWeekYear(now);

        const snapshotDbName = `${dbName}-week-${week}-${year}`;

        const srcClient = await ClientDB._connectExternal(sourceUri);
        const tgtClient = await ClientDB._connectExternal(targetUri);

        const src = srcClient.db(dbName);
        const tgt = tgtClient.db(snapshotDbName);

        // ⭐ Drop existing snapshot to prevent duplicate key errors
        await tgt.dropDatabase();

        const collections = await src.listCollections().toArray();
        const report: any[] = [];
        let totalDocs = 0;

        for (const col of collections) {
            const name = col.name;
            const srcCol = src.collection(name);
            const tgtCol = tgt.collection(name);

            const docs = await srcCol.find({}).toArray();
            if (docs.length > 0) {
                await tgtCol.insertMany(docs);
                totalDocs += docs.length;
            }

            report.push({
                collection: name,
                inserted: docs.length,
            });
        }

        await srcClient.close();
        await tgtClient.close();

        return {
            mode: "full-sync",
            sourceDb: dbName,
            snapshotDb: snapshotDbName,
            totalDocs,
            report,
        };
    }

    static async fullSyncManyDatabases(params: {
        targetURI: string;
        dbs: string[];
        keepWeeks?: number;
    }) {
        const { targetURI, dbs } = params;
        const keepWeeks = params.keepWeeks ?? 26;

        const sourceUri = process.env.MONGODB_URI!;
        const results: any[] = [];

        for (const dbName of dbs) {
            const r = await ClientDB.fullSyncOneDatabase({
                sourceUri,
                targetUri: targetURI,
                dbName,
            });
            results.push(r);
        }

        await ClientDB.cleanupSnapshots({
            targetURI,
            dbs,
            keepWeeks,
        });

        return {
            mode: "full-sync",
            results,
        };
    }

    static async cleanupSnapshots(params: {
        targetURI: string;
        dbs: string[];
        keepWeeks?: number;
    }) {
        const { targetURI, dbs } = params;
        const keepWeeks = params.keepWeeks ?? 26;

        const client = await ClientDB._connectExternal(targetURI);
        const admin = client.db().admin();

        const all = await admin.listDatabases();
        const names: string[] = all.databases.map(
            (d: { name: string }) => d.name
        );

        const cutoff = subWeeks(new Date(), keepWeeks);
        const cutoffWeek = getISOWeek(cutoff);
        const cutoffYear = getISOWeekYear(cutoff);

        for (const base of dbs) {
            const prefix = `${base}-week-`;

            const matches = names.filter((name) => name.startsWith(prefix));

            for (const dbName of matches) {
                const parts = dbName.replace(prefix, "").split("-");
                const week = Number(parts[0]);
                const year = Number(parts[1]);

                const isOld =
                    year < cutoffYear ||
                    (year === cutoffYear && week < cutoffWeek);

                if (isOld) {
                    console.log(`🗑 Removing old snapshot: ${dbName}`);
                    await client.db(dbName).dropDatabase();
                }
            }
        }

        await client.close();
    }
}

export default ClientDB;
export { ObjectId };
