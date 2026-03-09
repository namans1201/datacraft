import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { Button } from '@/components/common/Button';
export const LoginForm = ({ onSubmit }) => {
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    return (_jsxs("form", { onSubmit: (e) => {
            e.preventDefault();
            onSubmit(email, password);
        }, className: "space-y-4", children: [_jsx("div", { children: _jsx("input", { type: "email", placeholder: "Email address", required: true, className: "w-full rounded-lg border border-gray-300 px-4 py-2.5 text-sm\n                     focus:border-primary-500 focus:ring-2 focus:ring-primary-100 outline-none", value: email, onChange: (e) => setEmail(e.target.value) }) }), _jsx("div", { children: _jsx("input", { type: "password", placeholder: "Password", required: true, className: "w-full rounded-lg border border-gray-300 px-4 py-2.5 text-sm\n                     focus:border-primary-500 focus:ring-2 focus:ring-primary-100 outline-none", value: password, onChange: (e) => setPassword(e.target.value) }) }), _jsx(Button, { type: "submit", variant: "primary", className: "w-full", children: "Sign In" })] }));
};
