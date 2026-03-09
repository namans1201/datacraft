import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { User, LogOut } from 'lucide-react';
import { Button } from '@/components/common/Button';
import { useUIStore } from '@/store/useUIStore';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '@/store/useAuthStore';
import toast from 'react-hot-toast';
export const Header = () => {
    const navigate = useNavigate();
    const { toggleChat } = useUIStore();
    const logout = useAuthStore((s) => s.logout);
    const handleLogout = () => {
        logout();
        toast.success('Logged out successfully');
        navigate('/login', { replace: true });
    };
    return (_jsx("header", { className: "bg-white shadow-sm border-b border-gray-200 px-6 py-4", children: _jsxs("div", { className: "flex items-center justify-between", children: [_jsxs("div", { className: "flex items-center gap-2", children: [_jsx("span", { className: "text-2xl", children: "\u26A1" }), _jsx("h1", { className: "text-2xl font-bold text-primary-600", children: "Navisphere - Agentic Data Ingestion" })] }), _jsxs("div", { className: "flex items-center gap-4", children: [_jsx(Button, { variant: "primary", size: "sm", icon: _jsx(LogOut, { className: "w-4 h-4" }), onClick: handleLogout, children: "Logout" }), _jsxs("div", { className: "flex items-center gap-2 px-3 py-2 bg-gray-100 rounded-lg", children: [_jsx(User, { className: "w-4 h-4 text-gray-600" }), _jsx("span", { className: "text-sm text-gray-700", children: "User Profile" })] })] })] }) }));
};
