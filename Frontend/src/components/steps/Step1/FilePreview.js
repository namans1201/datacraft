import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState, useMemo } from 'react';
import { Shield, Eye, EyeOff } from 'lucide-react';
import { DataTable } from '@/components/common/DataTable';
// FIX 1: Added missing Button import
import { Button } from '@/components/common/Button';
const SENSITIVITY_OPTIONS = ["NON_SENSITIVE", "PII", "PCI", "PHI"];
export const FilePreview = ({ fileName, data }) => {
    const [localSensitivity, setLocalSensitivity] = useState(data.sensitivity || {});
    const [maskEnabled, setMaskEnabled] = useState(false);
    const handleSensitivityChange = (col, value) => {
        setLocalSensitivity(prev => ({ ...prev, [col]: value }));
    };
    const maskValue = (val, col) => {
        if (val === null || val === undefined || val === '')
            return "-";
        if (!maskEnabled || localSensitivity[col] === "NON_SENSITIVE")
            return val;
        const str = String(val);
        if (str.length <= 2)
            return "**";
        return `${str[0]}***${str[str.length - 1]}`;
    };
    const tableColumns = useMemo(() => {
        return data.columns.map((col, index) => ({
            key: col,
            header: col,
            render: (row) => {
                let value;
                if (typeof row !== 'object' || row === null) {
                    value = row;
                }
                else {
                    const actualKey = Object.keys(row).find(key => key.toLowerCase() === col.toLowerCase());
                    value = actualKey ? row[actualKey] : row[index];
                }
                return (_jsx("span", { className: "font-mono text-sm text-gray-700", children: maskValue(value, col) }));
            }
        }));
    }, [data.columns, maskEnabled, localSensitivity, maskValue]);
    return (_jsxs("div", { className: "space-y-6 mb-10", children: [_jsxs("div", { className: "flex justify-between items-center", children: [_jsx("h4", { className: "text-lg font-bold text-gray-900", children: fileName }), _jsxs(Button, { variant: maskEnabled ? "danger" : "secondary", size: "sm", onClick: () => setMaskEnabled(!maskEnabled), className: "flex items-center gap-2", children: [maskEnabled ? _jsx(EyeOff, { className: "w-4 h-4" }) : _jsx(Eye, { className: "w-4 h-4" }), maskEnabled ? "Masking On" : "Masking Off"] })] }), _jsxs("div", { className: "border border-gray-200 rounded-lg overflow-hidden shadow-sm", children: [_jsxs("div", { className: "bg-gray-50 px-4 py-2 border-b border-gray-200 flex items-center gap-2", children: [_jsx(Shield, { className: "w-4 h-4 text-gray-500" }), _jsx("span", { className: "text-xs font-bold text-gray-500 uppercase", children: "Column Classification (AI Suggested)" })] }), _jsx("div", { className: "overflow-x-auto", children: _jsx("table", { className: "w-full", children: _jsx("tbody", { className: "bg-white", children: _jsx("tr", { className: "divide-x divide-gray-100", children: data.columns.map(col => (_jsx("td", { className: "px-4 py-3 min-w-[150px]", children: _jsxs("div", { className: "flex flex-col gap-1", children: [_jsx("span", { className: "text-[10px] font-bold text-gray-400 uppercase truncate", children: col }), _jsx("select", { value: localSensitivity[col] || "NON_SENSITIVE", onChange: (e) => handleSensitivityChange(col, e.target.value), className: `text-xs font-semibold rounded-md border-gray-200 p-1.5 focus:ring-2 focus:ring-primary-500 w-full cursor-pointer transition-colors ${localSensitivity[col] !== 'NON_SENSITIVE'
                                                        ? 'bg-orange-50 text-orange-700 border-orange-200'
                                                        : 'bg-emerald-50 text-emerald-700 border-emerald-200'}`, children: SENSITIVITY_OPTIONS.map(opt => _jsx("option", { value: opt, children: opt }, opt)) })] }) }, col))) }) }) }) })] }), _jsxs("div", { className: "border border-gray-200 rounded-lg overflow-hidden shadow-sm", children: [_jsx("div", { className: "bg-gray-50 px-4 py-2 border-b border-gray-200", children: _jsx("span", { className: "text-xs font-bold text-gray-500 uppercase", children: "Data Preview (Sample)" }) }), _jsx(DataTable, { columns: tableColumns, data: data.data, className: "bg-white" })] })] }));
};
