import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { LayoutDashboard, Workflow, Eye, Table as TableIcon } from 'lucide-react';
import { clsx } from 'clsx';
import { Card } from '@/components/common/Card';
import { Input } from '@/components/common/Input';
import { Button } from '@/components/common/Button';
import { databricksApi } from '@/api/databricks';
import toast from 'react-hot-toast';
export const Sidebar = () => {
    const [activeNav, setActiveNav] = useState('pipeline');
    const [metadataForm, setMetadataForm] = useState({
        catalog: '',
        schema: '',
        token: '',
    });
    const [isLoading, setIsLoading] = useState(false);
    const [tables, setTables] = useState([]);
    const handleShowTables = async () => {
        if (!metadataForm.catalog || !metadataForm.schema || !metadataForm.token) {
            toast.error('Please fill all fields');
            return;
        }
        setIsLoading(true);
        setTables([]); // Clear previous results
        try {
            const response = await databricksApi.getTableMetadata(metadataForm.catalog, metadataForm.schema, metadataForm.token);
            if (response.success) {
                toast.success('Tables loaded successfully');
                setTables(response.data || []);
            }
            else {
                toast.error(response.error || 'Failed to load tables');
            }
        }
        catch (error) {
            console.error("Frontend Error:", error);
            toast.error('Connection error. Check console for details.');
        }
        finally {
            setIsLoading(false);
        }
    };
    return (_jsx("div", { className: "w-72 bg-white border-r border-gray-200 h-full overflow-y-auto", children: _jsxs("div", { className: "p-4", children: [_jsx("h3", { className: "text-sm font-semibold text-gray-700 mb-3", children: "Navigation" }), _jsxs("div", { className: "space-y-1", children: [_jsxs("button", { onClick: () => setActiveNav('pipeline'), className: clsx('w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors', activeNav === 'pipeline' ? 'bg-primary-50 text-primary-700' : 'text-gray-700 hover:bg-gray-50'), children: [_jsx(Workflow, { className: "w-4 h-4" }), "Pipeline Builder"] }), _jsxs("button", { onClick: () => setActiveNav('dashboard'), className: clsx('w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors', activeNav === 'dashboard' ? 'bg-primary-50 text-primary-700' : 'text-gray-700 hover:bg-gray-50'), children: [_jsx(LayoutDashboard, { className: "w-4 h-4" }), "Dashboard"] })] }), _jsxs("div", { className: "mt-8", children: [_jsxs("h3", { className: "text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2", children: [_jsx(Eye, { className: "w-4 h-4" }), "Metadata Viewer"] }), _jsx(Card, { padding: "sm", className: "bg-gray-50", children: _jsxs("div", { className: "space-y-3", children: [_jsx(Input, { label: "Catalog Name", placeholder: "Enter catalog", value: metadataForm.catalog, onChange: (e) => setMetadataForm({ ...metadataForm, catalog: e.target.value }) }), _jsx(Input, { label: "Schema Name", placeholder: "Enter schema", value: metadataForm.schema, onChange: (e) => setMetadataForm({ ...metadataForm, schema: e.target.value }) }), _jsx(Input, { label: "Access Token", type: "password", placeholder: "Enter token", value: metadataForm.token, onChange: (e) => setMetadataForm({ ...metadataForm, token: e.target.value }) }), _jsx(Button, { variant: "primary", size: "sm", className: "w-full", onClick: handleShowTables, isLoading: isLoading, children: "Show Tables" })] }) }), _jsx("div", { className: "mt-6", children: tables.length > 0 ? (_jsxs("div", { className: "space-y-4", children: [_jsx("div", { className: "flex items-center justify-between border-b border-gray-100 pb-2", children: _jsxs("h4", { className: "text-[10px] font-bold text-gray-400 uppercase tracking-widest", children: ["Detected Tables (", tables.length, ")"] }) }), _jsx("div", { className: "space-y-3 pb-10", children: tables.map((item, idx) => (_jsx(Card, { padding: "sm", className: "bg-white border-gray-100 hover:border-primary-200 transition-all shadow-sm group", children: _jsxs("div", { className: "flex flex-col gap-1", children: [_jsxs("div", { className: "flex items-center gap-2 text-gray-900 group-hover:text-primary-600", children: [_jsx(TableIcon, { className: "w-3.5 h-3.5" }), _jsx("span", { className: "text-sm font-semibold truncate", children: item.Table })] }), _jsxs("div", { className: "mt-1.5 p-2 bg-gray-50 rounded border border-gray-100", children: [_jsx("span", { className: "text-[9px] font-bold text-gray-400 uppercase", children: "Columns" }), _jsx("p", { className: "text-[11px] text-gray-500 leading-relaxed mt-0.5 italic", children: item.Columns })] })] }) }, idx))) })] })) : (!isLoading && (_jsx("div", { className: "text-center py-10", children: _jsx("p", { className: "text-xs text-gray-400 italic", children: "No tables to display. Fill details and click fetch." }) }))) })] })] }) }));
};
