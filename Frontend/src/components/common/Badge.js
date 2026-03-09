import { jsx as _jsx } from "react/jsx-runtime";
import { clsx } from 'clsx';
export const Badge = ({ children, variant, size = 'sm' }) => {
    const variants = {
        PII: 'bg-danger-50 text-danger-700 border-danger-200',
        PHI: 'bg-warning-50 text-warning-700 border-warning-200',
        PCI: 'bg-purple-50 text-purple-700 border-purple-200',
        NON_SENSITIVE: 'bg-gray-50 text-gray-700 border-gray-200',
        dimension: 'bg-primary-50 text-primary-700 border-primary-200',
        fact: 'bg-success-50 text-success-700 border-success-200',
        FAIL: 'bg-danger-50 text-danger-700 border-danger-200',
        DROP: 'bg-warning-50 text-warning-700 border-warning-200',
        LOG: 'bg-success-50 text-success-700 border-success-200',
    };
    const sizes = {
        sm: 'px-2 py-0.5 text-xs',
        md: 'px-3 py-1 text-sm',
    };
    return (_jsx("span", { className: clsx('inline-flex items-center rounded-md font-medium border', variants[variant], sizes[size]), children: children }));
};
