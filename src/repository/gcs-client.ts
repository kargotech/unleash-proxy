import { Storage } from "@google-cloud/storage";
import fs from "fs";
import path from "path";

export class GCSClient {
    private bucketName: string;

    constructor(bucketName: string) {
        if (!bucketName) {
            throw new Error("bucketName is required");
        }
        this.bucketName = bucketName;
    }

    private getStorage(): Storage {
        const base64Key = process.env.GCP_SA_KEY_B64;
        if (!base64Key) {
            throw new Error("Missing env var: GCP_SA_KEY_B64");
        }

        const keyJson = JSON.parse(Buffer.from(base64Key, "base64").toString("utf8"));

        return new Storage({
            credentials: {
                client_email: keyJson.client_email,
                private_key: keyJson.private_key,
            },
            projectId: keyJson.project_id,
        });
    }

    /* ============================================================
     *  NORMAL UPLOAD (SDK) — will fail if token refresh cannot reach googleapis.com
     * ============================================================ */
    public async uploadJsonToGCS(filePath: string, destination?: string): Promise<void> {
        if (!fs.existsSync(filePath)) {
            throw new Error(`File not found: ${filePath}`);
        }

        const destName = destination ?? path.basename(filePath);

        try {
            const storage = this.getStorage();
            await storage.bucket(this.bucketName).upload(filePath, {
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

    /* ============================================================
     *  NORMAL DOWNLOAD (SDK) — will fail if token refresh cannot reach googleapis.com
     * ============================================================ */
    public async readPublicJsonFromGCS<T = any>(objectName: string): Promise<T> {
        try {
            const storage = this.getStorage();
            const data = await storage
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

    /* ============================================================
     *  SIGNED URL GENERATION (server that CAN authenticate)
     * ============================================================ */

    /** Generate a signed URL for uploading JSON (PUT). */
    public async generateUploadSignedUrl(objectName: string, expiresInSeconds = 900): Promise<string> {
        const storage = this.getStorage();

        const [url] = await storage
            .bucket(this.bucketName)
            .file(objectName)
            .getSignedUrl({
                version: "v4",
                action: "write",
                expires: Date.now() + expiresInSeconds * 1000,
                contentType: "application/json",
            });

        console.log(`Generated signed upload URL (expires in ${expiresInSeconds}s): ${url}`);

        return url;
    }

    /** Generate a signed URL for downloading JSON (GET). */
    public async generateDownloadSignedUrl(objectName: string, expiresInSeconds = 900): Promise<string> {
        const storage = this.getStorage();

        const [url] = await storage
            .bucket(this.bucketName)
            .file(objectName)
            .getSignedUrl({
                version: "v4",
                action: "read",
                expires: Date.now() + expiresInSeconds * 1000,
            });

        console.log(`Generated signed download URL (expires in ${expiresInSeconds}s): ${url}`);

        return url;
    }

    /* ============================================================
     *  SIGNED URL OPERATIONS (works in restricted GKE pods)
     * ============================================================ */

    /** Upload JSON content directly using a signed URL. */
    public async uploadJsonUsingSignedUrl(url: string, json: any): Promise<void> {
        const body = JSON.stringify(json);

        const res = await fetch(url, {
            method: "PUT",
            headers: {
                "Content-Type": "application/json",
                "Content-Length": Buffer.byteLength(body).toString(),
            },
            body,
        });

        if (!res.ok) {
            throw new Error(`Signed URL upload failed: ${res.status} ${res.statusText}`);
        }

        console.log("Upload via signed URL succeeded");
    }

    /** Download JSON content using a signed URL. */
    public async downloadJsonUsingSignedUrl<T = any>(url: string): Promise<T> {
        const res = await fetch(url);

        if (!res.ok) {
            throw new Error(`Signed URL download failed: ${res.status} ${res.statusText}`);
        }

        return (await res.json()) as T;
    }
}
