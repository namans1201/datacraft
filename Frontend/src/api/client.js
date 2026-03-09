/// <reference types="vite/client" />
import axios from 'axios';
const BASE_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';
/**
 * Safely extracts an error message from unknown errors
 */
function parseApiError(error) {
    if (axios.isAxiosError(error)) {
        return (error.response?.data?.message ??
            error.message ??
            'Request failed');
    }
    if (error instanceof Error) {
        return error.message;
    }
    return 'Unknown error occurred';
}
class ApiClient {
    constructor() {
        Object.defineProperty(this, "client", {
            enumerable: true,
            configurable: true,
            writable: true,
            value: void 0
        });
        this.client = axios.create({
            baseURL: BASE_URL,
            timeout: 60000,
            headers: {
                'Content-Type': 'application/json',
            },
        });
        this.client.interceptors.response.use((response) => response, (error) => {
            console.error('API Error:', error.response?.data ?? error.message);
            return Promise.reject(error);
        });
    }
    async get(url, config = {}) {
        try {
            const response = await this.client.get(url, config);
            return { success: true, data: response.data };
        }
        catch (error) {
            return {
                success: false,
                error: parseApiError(error),
            };
        }
    }
    async post(url, data, config = {}) {
        try {
            const response = await this.client.post(url, data, config);
            return { success: true, data: response.data };
        }
        catch (error) {
            return {
                success: false,
                error: parseApiError(error),
            };
        }
    }
    async postFormData(url, formData) {
        try {
            const response = await this.client.post(url, formData, {
                headers: {
                    'Content-Type': 'multipart/form-data',
                },
            });
            return { success: true, data: response.data };
        }
        catch (error) {
            return {
                success: false,
                error: parseApiError(error),
            };
        }
    }
}
export const apiClient = new ApiClient();
