import { jsx as _jsx } from "react/jsx-runtime";
import { Navigate } from 'react-router-dom';
import { useAuthStore } from '@/store/useAuthStore';
export const ProtectedRoute = ({ children }) => {
    const user = useAuthStore((s) => s.user);
    if (!user) {
        return _jsx(Navigate, { to: "/login", replace: true });
    }
    return children;
};
