import { create } from 'zustand';
export const useAuthStore = create((set) => ({
    user: null,
    accessToken: null,
    isAuthenticated: false,
    login: (user, accessToken) => set({
        user,
        accessToken,
        isAuthenticated: true,
    }),
    logout: () => set({
        user: null,
        accessToken: null,
        isAuthenticated: false,
    }),
}));
