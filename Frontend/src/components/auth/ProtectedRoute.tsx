import { Navigate } from 'react-router-dom';
import { useAuthStore } from '@/store/useAuthStore';
import { JSX } from 'react';

export const ProtectedRoute = ({ children }: { children: JSX.Element }) => {
  const user = useAuthStore((s) => s.user);

  if (!user) {
    return <Navigate to="/login" replace />;
  }

  return children;
};
