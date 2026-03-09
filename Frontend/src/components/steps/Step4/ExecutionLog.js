import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { CheckCircle, XCircle, Loader2, AlertCircle } from 'lucide-react';
import { clsx } from 'clsx';
export const ExecutionLog = ({ status, logs }) => {
    const statusConfig = {
        RUNNING: {
            icon: Loader2,
            color: 'text-primary-600',
            bg: 'bg-primary-50',
            border: 'border-primary-200',
            label: 'Running...',
            animate: true,
        },
        SUCCESS: {
            icon: CheckCircle,
            color: 'text-success-600',
            bg: 'bg-success-50',
            border: 'border-success-200',
            label: 'Completed Successfully',
            animate: false,
        },
        FAILED: {
            icon: XCircle,
            color: 'text-danger-600',
            bg: 'bg-danger-50',
            border: 'border-danger-200',
            label: 'Execution Failed',
            animate: false,
        },
        NOT_STARTED: {
            icon: AlertCircle,
            color: 'text-gray-600',
            bg: 'bg-gray-50',
            border: 'border-gray-200',
            label: 'Not Started',
            animate: false,
        },
    };
    const config = statusConfig[status];
    const Icon = config.icon;
    return (_jsxs("div", { className: clsx('p-4 border rounded-lg', config.bg, config.border), children: [_jsxs("div", { className: "flex items-center gap-2 mb-3", children: [_jsx(Icon, { className: clsx('w-5 h-5', config.color, config.animate && 'animate-spin') }), _jsx("h4", { className: clsx('text-sm font-semibold', config.color), children: config.label })] }), logs.length > 0 && (_jsx("div", { className: "space-y-2 max-h-64 overflow-y-auto", children: logs.map((log, idx) => (_jsxs("div", { className: clsx('text-xs p-2 rounded', log.level === 'error' && 'bg-danger-100 text-danger-900', log.level === 'warning' && 'bg-warning-100 text-warning-900', log.level === 'info' && 'bg-white text-gray-700'), children: [_jsxs("span", { className: "font-medium", children: ["[", log.timestamp, "]"] }), " ", log.message] }, idx))) }))] }));
};
