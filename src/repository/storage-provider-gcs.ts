import fs from 'fs';
import path from 'path';
import { join } from 'path';
import { promises } from 'fs';
import { safeName } from '../helpers';
import { StorageProvider } from './storage-provider';
import { Storage } from '@google-cloud/storage';

const { writeFile } = promises;

export default class GcsStorageProvider<T> implements StorageProvider<T> {
    private backupPath: string;

    constructor(backupPath: string) {
        if (!backupPath) {
            throw new Error('backupPath is required');
        }
        this.backupPath = backupPath;
    }

    /**
       * Upload JSON file to Google Cloud Storage without gzip.
       *
       * @param filePath - local JSON file path
       * @param destination - object name in bucket (optional)
       */
    private async uploadJsonToGCS(
        filePath: string,
        destination?: string
    ): Promise<void> {
        // Load base64 service account key from environment variable
        const base64Key = process.env.GCP_SA_KEY_B64;
        if (!base64Key) {
            throw new Error("Missing env var: GCP_SA_KEY_B64");
        }

        const bucketName = process.env.GCP_BUCKET_NAME;
        if (!bucketName) {
            throw new Error("Missing env var: GCP_BUCKET_NAME");
        }

        // Decode base64 JSON key
        const keyJson = JSON.parse(Buffer.from(base64Key, "base64").toString("utf8"));

        // Authenticate using decoded credentials
        const storage = new Storage({
            credentials: {
                client_email: keyJson.client_email,
                private_key: keyJson.private_key,
            },
            projectId: keyJson.project_id,
        });

        if (!fs.existsSync(filePath)) {
            throw new Error(`File not found: ${filePath}`);
        }

        const destName = destination ?? path.basename(filePath);

        storage.bucket(bucketName).upload(filePath, {
            destination: destName,
            gzip: false,
            metadata: {
                contentType: "application/json",
                cacheControl: "no-cache",
            },
        });
    }

    /**
     * Reads a public JSON file from Google Cloud Storage.
     *
     * @param bucketName - Name of the bucket
     * @param objectName - Name/path of the object
     */
    private async readPublicJsonFromGCS(
        objectName: string
    ): Promise<any> {
        const bucketName = process.env.GCP_BUCKET_NAME;
        if (!bucketName) {
            throw new Error("Missing env var: GCP_BUCKET_NAME");
        }
        const url = `https://storage.googleapis.com/${bucketName}/${objectName}`;

        const response = await fetch(url);
        if (!response.ok) {
            console.error(`Failed to fetch JSON from GCS: ${url} with response ${response.status} ${response.statusText}`);
            throw new Error(`Failed to fetch JSON from ${url}: ${response.statusText}`);
        }

        const data = await response.json();
        console.log(`Successfully fetched JSON from GCS: ${url}`);
        console.log(data);
        return data;
    }

    private getPath(key: string): string {
        return join(this.backupPath, `/unleash-backup-${safeName(key)}.json`);
    }

    async set(key: string, data: T): Promise<void> {
        await writeFile(this.getPath(key), JSON.stringify(data));
        return this.uploadJsonToGCS(this.getPath(key));
    }

    async get(key: string): Promise<T | undefined> {
        const path = this.getPath(key);
        console.log(`Fetching backup from GCS path: ${path}`);
        let data;
        try {
            data = await this.readPublicJsonFromGCS(path);
        } catch (error: any) {
            if (error.code !== 'ENOENT') {
                throw error;
            } else {
                return undefined;
            }
        }

        console.log(`Fetched backup data: ${JSON.stringify(data)}`);

        return data;
    }
}