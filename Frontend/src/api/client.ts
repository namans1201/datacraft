/// <reference types="vite/client" />

import axios, { AxiosError, AxiosInstance } from 'axios';
import { ApiResponse } from '../types/api';

const BASE_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

/**
 * Safely extracts an error message from unknown errors
 */
function parseApiError(error: unknown): string {
  if (axios.isAxiosError(error)) {
    return (
      (error.response?.data as { message?: string })?.message ??
      error.message ??
      'Request failed'
    );
  }

  if (error instanceof Error) {
    return error.message;
  }

  return 'Unknown error occurred';
}

class ApiClient {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: BASE_URL,
      timeout: 60_000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    this.client.interceptors.response.use(
      (response) => response,
      (error: AxiosError) => {
        console.error('API Error:', error.response?.data ?? error.message);
        return Promise.reject(error);
      }
    );
  }

  async get<T>(
    url: string,
    config: Record<string, unknown> = {}
  ): Promise<ApiResponse<T>> {
    try {
      const response = await this.client.get<T>(url, config);
      return { success: true, data: response.data };
    } catch (error: unknown) {
      return {
        success: false,
        error: parseApiError(error),
      };
    }
  }

  async post<T>(
    url: string,
    data?: unknown,
    config: Record<string, unknown> = {}
  ): Promise<ApiResponse<T>> {
    try {
      const response = await this.client.post<T>(url, data, config);
      return { success: true, data: response.data };
    } catch (error: unknown) {
      return {
        success: false,
        error: parseApiError(error),
      };
    }
  }

  async postFormData<T>(
    url: string,
    formData: FormData
  ): Promise<ApiResponse<T>> {
    try {
      const response = await this.client.post<T>(url, formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });
      return { success: true, data: response.data };
    } catch (error: unknown) {
      return {
        success: false,
        error: parseApiError(error),
      };
    }
  }
}

export const apiClient = new ApiClient();
