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

    private getBucketName(): string {
        const bucketName = process.env.GCP_BUCKET_NAME || "";
        if (!bucketName) {
            throw new Error("Missing env var: GCP_BUCKET_NAME");
        }
        return bucketName;
    }

    async set(key: string, data: T): Promise<void> {
        await writeFile(this.getPath(key), JSON.stringify(data));
        const gcs = new GCSClient(this.getBucketName());
        const uploadUrl = await gcs.generateUploadSignedUrl(this.getPath(key));
        return gcs.uploadJsonUsingSignedUrl(uploadUrl, data);
    }

    async get(key: string): Promise<T | undefined> {
        const path = this.getPath(key);
        console.log(`Fetching backup from GCS path: ${path}`);

        const MAX_RETRIES = 5;
        const BASE_DELAY_MS = 5000;
        let attempt = 0;

        while (attempt < MAX_RETRIES) {
            try {
                const gcs = new GCSClient(this.getBucketName());
                const downloadUrl = await gcs.generateDownloadSignedUrl(path);
                const data = await gcs.downloadJsonUsingSignedUrl(downloadUrl);
                console.log(`Fetched backup data: ${JSON.stringify(data)}`);
                return data;
            } catch (error: any) {
                // Special case: return undefined if object not found
                if (error?.status === 404) {
                    console.log("Backup not found (404)");
                    return undefined;
                }

                attempt++;
                console.error(`Attempt ${attempt} failed:`, error?.message || error);

                if (attempt >= MAX_RETRIES) {
                    console.error(`All ${MAX_RETRIES} attempts failed.`);
                    throw error; // rethrow final error
                }

                // Wait with exponential backoff (5s, 10s, 20s, 40s...)
                const delayMs = BASE_DELAY_MS * Math.pow(2, attempt - 1);
                console.log(`Retrying in ${delayMs / 1000}s...`);

                await new Promise(resolve => setTimeout(resolve, delayMs));
            }
        }

        return undefined; // unreachable but satisfies TS
    }
}