import { apiClient } from './client';
import { AuthProvider } from '@/components/auth/auth.types';

/* =========================
   Types
========================= */

export interface AuthUser {
    id: string;
    email: string;
    provider: AuthProvider;
}

export interface LoginResponse {
    user: AuthUser;
    accessToken: string;
}

/**
 * Local login payload
 */
export interface LocalLoginRequest {
    email: string;
    password: string;
}

/**
 * Microsoft login payload
 */
export interface MicrosoftLoginRequest {
    access_token: string;
}

export interface SignupRequest {
    email: string;
    password: string;
}

/* =========================
   Auth API
========================= */

export const authApi = {
    /**
     * Local email/password login
     */
    async loginLocal(payload: LocalLoginRequest): Promise<LoginResponse> {
        const result = await apiClient.post<LoginResponse>(
            '/auth/login',
            payload
        );

        if (!result.success) {
            throw new Error(result.error ?? 'Login failed');
        }

        if (!result.data) {
            throw new Error('No login data received');
        }

        return result.data;
    },

    async loginMicrosoft(accessToken: string): Promise<LoginResponse> {
        const result = await apiClient.post<LoginResponse>(
            '/auth/login/microsoft',
            { access_token: accessToken }
        );

        if (!result.success) {
            throw new Error(result.error ?? 'Microsoft login failed');
        }

        if (!result.data) {
            throw new Error('No login data received');
        }

        return result.data;
    },

    async signup(payload: SignupRequest): Promise<void> {
        const result = await apiClient.post('/auth/signup', payload);
        if (!result.success) {
            throw new Error(result.error ?? 'Signup failed');
        }
    },

    /**
     * Logout
     * (Optional backend call – useful later for refresh tokens)
     */
    async logout(): Promise<void> {
        const result = await apiClient.post('/auth/logout');

        if (!result.success) {
            throw new Error(result.error ?? 'Logout failed');
        }
    },
};
