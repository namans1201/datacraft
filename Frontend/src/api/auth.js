import { apiClient } from './client';
/* =========================
   Auth API
========================= */
export const authApi = {
    /**
     * Local email/password login
     */
    async loginLocal(payload) {
        const result = await apiClient.post('/auth/login', payload);
        if (!result.success) {
            throw new Error(result.error ?? 'Login failed');
        }
        if (!result.data) {
            throw new Error('No login data received');
        }
        return result.data;
    },
    async loginMicrosoft(accessToken) {
        const result = await apiClient.post('/auth/login/microsoft', { access_token: accessToken });
        if (!result.success) {
            throw new Error(result.error ?? 'Microsoft login failed');
        }
        if (!result.data) {
            throw new Error('No login data received');
        }
        return result.data;
    },
    async signup(payload) {
        const result = await apiClient.post('/auth/signup', payload);
        if (!result.success) {
            throw new Error(result.error ?? 'Signup failed');
        }
    },
    /**
     * Logout
     * (Optional backend call – useful later for refresh tokens)
     */
    async logout() {
        const result = await apiClient.post('/auth/logout');
        if (!result.success) {
            throw new Error(result.error ?? 'Logout failed');
        }
    },
};
