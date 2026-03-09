export type AuthProvider = 'local' | 'microsoft';

export interface AuthUser {
  id: string;
  email: string;
  provider: AuthProvider;
}
