declare module "afridho-mongodb" {
    export class ClientDB {
        constructor(collectionName: string);
        connect(): Promise<void>;
        read(query: object): Promise<object | null>;
        readAll(): Promise<object[]>;
        insert(data: object): Promise<object>;
        insertMany(data: object[]): Promise<object>;
        update(query: object, data: object): Promise<object>;
        delete(query: object): Promise<object>;
        deleteMany(query: object): Promise<object>;
        find(query: object): Promise<object[]>;
        getStorageStats(): Promise<{
            storageSize: number;
            size: number;
            count: number;
        }>;
        close(): Promise<void>;
    }
}
