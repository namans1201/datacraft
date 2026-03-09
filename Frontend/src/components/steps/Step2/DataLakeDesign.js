import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useState } from 'react';
import { Layers, Sparkles, ArrowRight } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Select } from '@/components/common/Select';
import { Button } from '@/components/common/Button';
import { SilverMappingTable } from './SilverMappingTable';
import { GoldMappingTable } from './GoldMappingTable';
import { CustomSchemaUploader } from './CustomSchemaUploader';
import { mappingApi } from '@/api/mapping';
import { useAgentStore } from '@/store/useAgentStore';
import toast from 'react-hot-toast';
const mappingStandards = [
    { value: 'fhir', label: 'FHIR (Healthcare)' },
    { value: 'acord', label: 'ACORD (Insurance)' },
    { value: 'x12', label: 'X12 (EDI)' },
    { value: 'aids', label: 'AIDS' },
    { value: 'custom', label: 'Custom Schema Upload' },
];
export const DataLakeDesign = () => {
    const { catalog, schema, dbfs_path, // Ensure this is retrieved from store
    mapping_rows, gold_mapping_rows, setMappings, setGoldMappings, } = useAgentStore();
    const [standard, setStandard] = useState('fhir');
    const [customFile, setCustomFile] = useState(null);
    const [isGeneratingSilver, setIsGeneratingSilver] = useState(false);
    const [isGeneratingGold, setIsGeneratingGold] = useState(false);
    const handleGenerateSilverMappings = async () => {
        // Pull dbfs_path from the store alongside catalog and schema
        const { catalog, schema, dbfs_path } = useAgentStore.getState();
        if (!catalog || !schema || !dbfs_path) {
            toast.error("Required context (Catalog, Schema, or Path) is missing. Re-run Step 1.");
            return;
        }
        setIsGeneratingSilver(true);
        try {
            const response = await mappingApi.generateSilverMappings(standard, catalog, schema, dbfs_path, customFile || undefined);
            // Handle Axios response wrapping
            const result = response.data || response;
            if (result.success) {
                toast.success('Silver mappings generated!');
                setMappings(result.mapping_rows || []);
            }
            else {
                toast.error(result.message || 'Failed to generate silver mappings');
            }
        }
        catch (error) {
            // This captures the 404 "State not found" from the backend
            const detail = error.response?.data?.detail || "Mapping error";
            toast.error(typeof detail === 'string' ? detail : "Internal Server Error");
        }
        finally {
            setIsGeneratingSilver(false);
        }
    };
    const handleGenerateGoldMappings = async () => {
        if (mapping_rows.length === 0) {
            toast.error('Please generate Silver mappings first');
            return;
        }
        if (!catalog || !schema) {
            toast.error("Context missing. Please ensure Catalog and Schema are set.");
            return;
        }
        setIsGeneratingGold(true);
        try {
            // 3. Pass catalog and schema to Gold mapping API
            const response = await mappingApi.generateGoldMappings(catalog, schema);
            const result = response.data || response;
            if (result.success) {
                toast.success('Gold mappings generated successfully');
                setGoldMappings(result.gold_mapping_rows || []);
            }
            else {
                toast.error(result.message || 'Failed to generate gold mappings');
            }
        }
        catch (error) {
            console.error("Gold Mapping Error:", error);
            toast.error(error.response?.data?.detail || "Error generating gold mappings");
        }
        finally {
            setIsGeneratingGold(false);
        }
    };
    return (_jsxs("div", { className: "space-y-6", children: [_jsxs("div", { children: [_jsx("h2", { className: "text-2xl font-bold text-gray-900 mb-2", children: "Step 3: Data Lake Design" }), _jsx("p", { className: "text-gray-600", children: "Map your raw data to standardized schemas using industry standards or custom mappings." })] }), _jsxs(Card, { children: [_jsxs("div", { className: "flex items-center gap-2 mb-4", children: [_jsx(Layers, { className: "w-5 h-5 text-primary-600" }), _jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Bronze \u2192 Silver Mapping" })] }), _jsxs("div", { className: "space-y-4", children: [_jsx(Select, { label: "Select Mapping Standard", options: mappingStandards, value: standard, onChange: (e) => setStandard(e.target.value) }), standard === 'custom' && (_jsx(CustomSchemaUploader, { onFileSelect: setCustomFile, selectedFile: customFile })), _jsx(Button, { variant: "primary", onClick: handleGenerateSilverMappings, isLoading: isGeneratingSilver, icon: _jsx(Sparkles, { className: "w-4 h-4" }), children: "Generate Silver Mappings" })] })] }), mapping_rows.length > 0 && (_jsxs(Card, { children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Bronze \u2192 Silver Mappings" }), _jsxs("span", { className: "text-sm text-gray-500", children: [mapping_rows.length, " mappings"] })] }), _jsx(SilverMappingTable, { mappings: mapping_rows })] })), mapping_rows.length > 0 && (_jsxs(_Fragment, { children: [_jsx("div", { className: "flex items-center justify-center py-4", children: _jsx(ArrowRight, { className: "w-6 h-6 text-gray-400" }) }), _jsxs(Card, { children: [_jsxs("div", { className: "flex items-center gap-2 mb-4", children: [_jsx(Layers, { className: "w-5 h-5 text-success-600" }), _jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Silver \u2192 Gold Mapping" })] }), _jsx("p", { className: "text-sm text-gray-600 mb-4", children: "Create analytical gold layer tables with aggregated metrics and dimensional models." }), _jsx(Button, { variant: "primary", onClick: handleGenerateGoldMappings, isLoading: isGeneratingGold, icon: _jsx(Sparkles, { className: "w-4 h-4" }), children: "Generate Gold Mappings" })] })] })), gold_mapping_rows.length > 0 && (_jsxs(Card, { children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Silver \u2192 Gold Mappings" }), _jsxs("span", { className: "text-sm text-gray-500", children: [gold_mapping_rows.length, " mappings"] })] }), _jsx(GoldMappingTable, { mappings: gold_mapping_rows })] }))] }));
};
