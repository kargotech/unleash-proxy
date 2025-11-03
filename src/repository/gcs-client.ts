import { Storage } from "@google-cloud/storage";
import fs from "fs";
import path from "path";

export class GCSClient {
    private storage: Storage;
    private bucketName: string;

    constructor() {
        const base64Key = process.env.GCP_SA_KEY_B64;
        if (!base64Key) {
            throw new Error("Missing env var: GCP_SA_KEY_B64");
        }

        this.bucketName = process.env.GCP_BUCKET_NAME || "";
        if (!this.bucketName) {
            throw new Error("Missing env var: GCP_BUCKET_NAME");
        }

        // Decode base64 JSON key
        const keyJson = JSON.parse(Buffer.from(base64Key, "base64").toString("utf8"));

        // Initialize GCS client once
        this.storage = new Storage({
            credentials: {
                client_email: keyJson.client_email,
                private_key: keyJson.private_key,
            },
            projectId: keyJson.project_id,
        });
    }

    /**
     * Upload local JSON file to GCS without gzip.
     *
     * @param filePath - Local path to JSON file.
     * @param destination - Target object name in bucket.
     */
    public async uploadJsonToGCS(filePath: string, destination?: string): Promise<void> {
        if (!fs.existsSync(filePath)) {
            throw new Error(`File not found: ${filePath}`);
        }

        const destName = destination ?? path.basename(filePath);

        try {
            await this.storage.bucket(this.bucketName).upload(filePath, {
                destination: destName,
                gzip: false,
                metadata: {
                    contentType: "application/json",
                    cacheControl: "no-cache",
                },
            });

            console.log(`Uploaded to GCS: gs://${this.bucketName}/${destName}`);
        } catch (err: any) {
            console.error("Failed to upload JSON to GCS:", {
                filePath,
                bucket: this.bucketName,
                destName,
                error: err.message,
            });
            throw err;
        }
    }

    /**
     * Read a JSON file from a public GCS bucket.
     *
     * @param objectName - Path / object name in bucket.
     */
    public async readPublicJsonFromGCS<T = any>(objectName: string): Promise<T> {
        try {
            const data = await this.storage
                .bucket(this.bucketName)
                .file(objectName)
                .download();

            return JSON.parse(data.toString()) as T;
        } catch (err: any) {
            console.error("Failed to read JSON from GCS:", {
                bucket: this.bucketName,
                object: objectName,
                error: err.message,
            });
            throw err;
        }
    }
}
