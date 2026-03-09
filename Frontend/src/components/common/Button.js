import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Loader2 } from 'lucide-react';
import { clsx } from 'clsx';
export const Button = ({ children, variant = 'primary', size = 'md', isLoading = false, icon, className, disabled, ...props }) => {
    const baseStyles = 'inline-flex items-center justify-center gap-2 rounded-lg font-medium transition-all focus:outline-none focus:ring-2 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed';
    const variants = {
        primary: 'bg-primary-600 text-white hover:bg-primary-700 focus:ring-primary-500',
        secondary: 'bg-gray-200 text-gray-800 hover:bg-gray-300 focus:ring-gray-400',
        danger: 'bg-danger-500 text-white hover:bg-danger-600 focus:ring-danger-400',
        ghost: 'bg-transparent text-gray-700 hover:bg-gray-100 focus:ring-gray-300',
    };
    const sizes = {
        sm: 'px-3 py-1.5 text-sm',
        md: 'px-4 py-2 text-sm',
        lg: 'px-6 py-3 text-base',
    };
    return (_jsxs("button", { className: clsx(baseStyles, variants[variant], sizes[size], className), disabled: disabled || isLoading, ...props, children: [isLoading ? (_jsx(Loader2, { className: "w-4 h-4 animate-spin" })) : icon ? (icon) : null, children] }));
};
