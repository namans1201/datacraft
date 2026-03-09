import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { Check, Copy } from 'lucide-react';
export const CodeViewer = ({ code, language = 'python' }) => {
    const [copied, setCopied] = useState(false);
    const handleCopy = () => {
        navigator.clipboard.writeText(code);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    };
    return (_jsxs("div", { className: "relative", children: [_jsx("button", { onClick: handleCopy, className: "absolute top-3 right-3 p-2 bg-gray-800 hover:bg-gray-700 rounded-lg transition-colors z-10", children: copied ? (_jsx(Check, { className: "w-4 h-4 text-green-400" })) : (_jsx(Copy, { className: "w-4 h-4 text-gray-300" })) }), _jsx("div", { className: "bg-gray-900 rounded-lg p-4 overflow-x-auto", children: _jsx("pre", { className: "text-sm text-gray-100 font-mono", children: _jsx("code", { children: code }) }) })] }));
};
