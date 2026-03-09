import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import clsx from 'clsx';
function renderValue(value) {
    if (typeof value === 'string' ||
        typeof value === 'number' ||
        typeof value === 'boolean') {
        return value.toString();
    }
    if (value == null)
        return '-';
    return JSON.stringify(value);
}
export function DataTable({ columns, data, emptyMessage = 'No data available', className, }) {
    return (_jsx("div", { className: clsx('overflow-x-auto', className), children: _jsxs("table", { className: "w-full border-collapse", children: [_jsx("thead", { children: _jsx("tr", { className: "bg-gray-50 border-b-2 border-gray-200", children: columns.map((col) => (_jsx("th", { className: "px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider", style: { width: col.width }, children: col.header }, String(col.key)))) }) }), _jsx("tbody", { className: "bg-white divide-y divide-gray-200", children: data.length === 0 ? (_jsx("tr", { children: _jsx("td", { colSpan: columns.length, className: "px-4 py-8 text-center text-sm text-gray-500", children: emptyMessage }) })) : (data.map((row, idx) => (_jsx("tr", { className: "hover:bg-gray-50", children: columns.map((col) => {
                            const value = row[col.key];
                            return (_jsx("td", { className: "px-4 py-3 text-sm text-gray-900", children: col.render
                                    ? col.render(value, row)
                                    : renderValue(value) }, String(col.key)));
                        }) }, idx)))) })] }) }));
}
