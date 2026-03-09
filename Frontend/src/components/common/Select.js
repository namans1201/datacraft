import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { forwardRef } from 'react';
import { clsx } from 'clsx';
export const Select = forwardRef(({ label, error, options, className, ...props }, ref) => {
    return (_jsxs("div", { className: "w-full", children: [label && (_jsx("label", { className: "block text-sm font-medium text-gray-700 mb-2", children: label })), _jsx("select", { ref: ref, className: clsx('w-full px-3 py-2 border rounded-lg text-sm transition-colors appearance-none bg-white', 'focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent', error
                    ? 'border-danger-500 focus:ring-danger-500'
                    : 'border-gray-300', className), ...props, children: options.map((option) => (_jsx("option", { value: option.value, children: option.label }, option.value))) }), error && (_jsx("p", { className: "mt-1 text-sm text-danger-600", children: error }))] }));
});
Select.displayName = 'Select';
