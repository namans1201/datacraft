import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Upload, File, X } from 'lucide-react';
export const CustomSchemaUploader = ({ onFileSelect, selectedFile, }) => {
    const handleFileInput = (e) => {
        const files = Array.from(e.target.files || []);
        if (files.length > 0) {
            onFileSelect(files[0]);
        }
    };
    const handleRemove = () => {
        onFileSelect(null);
    };
    if (selectedFile) {
        return (_jsxs("div", { className: "flex items-center justify-between p-4 bg-primary-50 border border-primary-200 rounded-lg", children: [_jsxs("div", { className: "flex items-center gap-3", children: [_jsx(File, { className: "w-5 h-5 text-primary-600" }), _jsxs("div", { children: [_jsx("p", { className: "text-sm font-medium text-gray-900", children: selectedFile.name }), _jsxs("p", { className: "text-xs text-gray-500", children: [(selectedFile.size / 1024).toFixed(2), " KB"] })] })] }), _jsx("button", { onClick: handleRemove, className: "p-1 hover:bg-primary-100 rounded-full transition-colors", children: _jsx(X, { className: "w-4 h-4 text-gray-600" }) })] }));
    }
    return (_jsxs("label", { className: "block border-2 border-dashed border-gray-300 rounded-lg p-8 text-center cursor-pointer hover:border-primary-400 hover:bg-gray-50 transition-all", children: [_jsx("input", { type: "file", className: "hidden", accept: ".csv,.json,.parquet", onChange: handleFileInput }), _jsxs("div", { className: "flex flex-col items-center gap-2", children: [_jsx("div", { className: "p-3 bg-gray-100 rounded-full", children: _jsx(Upload, { className: "w-6 h-6 text-gray-600" }) }), _jsxs("div", { children: [_jsx("p", { className: "text-sm font-medium text-gray-900", children: "Upload Custom Schema" }), _jsx("p", { className: "text-xs text-gray-500 mt-1", children: "CSV, JSON, or Parquet format" })] })] })] }));
};
