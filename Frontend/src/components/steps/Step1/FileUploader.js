import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import React, { useCallback } from 'react';
import { Upload } from 'lucide-react';
import { clsx } from 'clsx';
export const FileUploader = ({ onFileSelect }) => {
    const [isDragging, setIsDragging] = React.useState(false);
    const handleDragOver = useCallback((e) => {
        e.preventDefault();
        setIsDragging(true);
    }, []);
    const handleDragLeave = useCallback((e) => {
        e.preventDefault();
        setIsDragging(false);
    }, []);
    const handleDrop = useCallback((e) => {
        e.preventDefault();
        setIsDragging(false);
        const files = Array.from(e.dataTransfer.files);
        if (files.length > 0) {
            onFileSelect(files[0]);
        }
    }, [onFileSelect]);
    const handleFileInput = (e) => {
        const files = Array.from(e.target.files || []);
        if (files.length > 0) {
            onFileSelect(files[0]);
        }
    };
    return (_jsxs("label", { className: clsx('block border-2 border-dashed rounded-lg p-12 text-center cursor-pointer transition-all', isDragging
            ? 'border-primary-500 bg-primary-50'
            : 'border-gray-300 hover:border-primary-400 hover:bg-gray-50'), onDragOver: handleDragOver, onDragLeave: handleDragLeave, onDrop: handleDrop, children: [_jsx("input", { type: "file", className: "hidden", accept: ".csv,.xlsx,.parquet,.json,.xml", onChange: handleFileInput }), _jsxs("div", { className: "flex flex-col items-center gap-3", children: [_jsx("div", { className: "p-4 bg-primary-50 rounded-full", children: _jsx(Upload, { className: "w-8 h-8 text-primary-600" }) }), _jsxs("div", { children: [_jsx("p", { className: "text-base font-medium text-gray-900 mb-1", children: "Drag & Drop or Click to Upload" }), _jsx("p", { className: "text-sm text-gray-500", children: "Supported: CSV, XLSX, Parquet, JSON, XML" })] })] })] }));
};
