import { promises } from 'fs';
import { safeName } from '../helpers';
import { StorageProvider } from './storage-provider';
import { GCSClient } from './gcs-client';

const { writeFile } = promises;

export default class GcsStorageProvider<T> implements StorageProvider<T> {
    private backupPath: string;

    constructor(backupPath: string) {
        if (!backupPath) {
            throw new Error('backupPath is required');
        }
        this.backupPath = backupPath;
    }

    private getPath(key: string): string {
        return `unleash-backup-${safeName(key)}.json`;
    }

    async set(key: string, data: T): Promise<void> {
        await writeFile(this.getPath(key), JSON.stringify(data));
        const gcs = new GCSClient();
        return gcs.uploadJsonToGCS(this.getPath(key));
    }

    async get(key: string): Promise<T | undefined> {
        const path = this.getPath(key);
        console.log(`Fetching backup from GCS path: ${path}`);

        try {
            const gcs = new GCSClient();
            const data = await gcs.readPublicJsonFromGCS(path);
            console.log(`Fetched backup data: ${JSON.stringify(data)}`);
            return data;
        } catch (error: any) {
            if (error.status === 404) {
                console.log("Backup not found (404)");
                return undefined;  // Object not found → treat as "no backup"
            }
            console.error("Error fetching backup:", error);
            throw error; // Other errors → rethrow
        }
    }
}