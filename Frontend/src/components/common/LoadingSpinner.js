import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Loader2 } from 'lucide-react';
import { clsx } from 'clsx';
export const LoadingSpinner = ({ size = 'md', text }) => {
    const sizes = {
        sm: 'w-4 h-4',
        md: 'w-8 h-8',
        lg: 'w-12 h-12',
    };
    return (_jsxs("div", { className: "flex flex-col items-center justify-center gap-3", children: [_jsx(Loader2, { className: clsx(sizes[size], 'animate-spin text-primary-600') }), text && _jsx("p", { className: "text-sm text-gray-600", children: text })] }));
};
