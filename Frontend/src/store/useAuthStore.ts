import { create } from 'zustand';
import { AuthUser, AuthProvider } from '../components/auth/auth.types';

interface AuthState {
  user: AuthUser | null;
  accessToken: string | null;
  isAuthenticated: boolean;

  login: (
    user: AuthUser,
    accessToken: string,
    refreshToken?: string
  ) => void;

  logout: () => void;
}


export const useAuthStore = create<AuthState>((set) => ({
  user: null,
  accessToken: null,
  isAuthenticated: false,

  login: (user, accessToken) =>
    set({
      user,
      accessToken,
      isAuthenticated: true,
    }),

  logout: () =>
    set({
      user: null,
      accessToken: null,
      isAuthenticated: false,
    }),
}));

