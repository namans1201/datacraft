import { jsx as _jsx } from "react/jsx-runtime";
import { clsx } from 'clsx';
export const Card = ({ children, className, padding = 'md' }) => {
    const paddingStyles = {
        none: '',
        sm: 'p-4',
        md: 'p-6',
        lg: 'p-8',
    };
    return (_jsx("div", { className: clsx('bg-white rounded-lg shadow-sm border border-gray-200', paddingStyles[padding], className), children: children }));
};
