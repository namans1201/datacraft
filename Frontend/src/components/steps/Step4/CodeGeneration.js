import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { Code, Shield, Sparkles, ChevronDown, ChevronUp } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Button } from '@/components/common/Button';
import { DQExpectationsPreview } from './DQExpectationsPreview';
import { CodeViewer } from './CodeViewer';
import { MaskingPanel } from './MaskingPanel';
import { codegenApi } from '@/api/codegen';
import { useAgentStore } from '@/store/useAgentStore';
import toast from 'react-hot-toast';
export const CodeGeneration = () => {
    const { catalog, schema_name, pyspark_code, dq_rules, masking_sql, setPySparkCode, setMaskingSQL, updateState, } = useAgentStore();
    const [isGenerating, setIsGenerating] = useState(false);
    const [showDQPreview, setShowDQPreview] = useState(false);
    const [dqExpectations, setDqExpectations] = useState([]);
    const [activeCodeTab, setActiveCodeTab] = useState('full');
    const handlePreviewDQ = async () => {
        if (!catalog || !schema_name) {
            toast.error("Catalog and Schema are missing. Please re-run 'Read Files'.");
            return;
        }
        try {
            const response = await codegenApi.previewDQExpectations(catalog, schema_name);
            const data = (response.data || []);
            setDqExpectations(data);
            setShowDQPreview(true);
        }
        catch (error) {
            toast.error(error.response?.data?.detail || 'Failed to load DQ expectations');
        }
    };
    const handleGenerateCode = async () => {
        if (!catalog || !schema_name) {
            toast.error("Catalog and Schema are missing.");
            return;
        }
        setIsGenerating(true);
        try {
            const response = await codegenApi.generateMedallionCode(catalog, schema_name);
            // Fix: Cast the response data so TS recognizes the properties
            const result = response.data;
            if (result && result.success) {
                toast.success('Medallion code generated successfully');
                setPySparkCode(result.pyspark_code || '');
                updateState({ dq_rules: result.message || '' });
            }
            else {
                toast.error(result?.message || 'Failed to generate code');
            }
        }
        catch (error) {
            toast.error(error.response?.data?.detail || 'Failed to generate code');
        }
        finally {
            setIsGenerating(false);
        }
    };
    return (_jsxs("div", { className: "space-y-6", children: [_jsxs("div", { children: [_jsx("h2", { className: "text-2xl font-bold text-gray-900 mb-2", children: "Step 5: Medallion PySpark Code Generation" }), _jsx("p", { className: "text-gray-600", children: "Generate production-ready PySpark Delta Live Tables code implementing the Medallion architecture." })] }), _jsxs(Card, { children: [_jsxs("button", { onClick: () => setShowDQPreview(!showDQPreview), className: "w-full flex items-center justify-between text-left", children: [_jsxs("div", { className: "flex items-center gap-2", children: [_jsx(Shield, { className: "w-5 h-5 text-primary-600" }), _jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Preview Data Quality Expectations" })] }), showDQPreview ? (_jsx(ChevronUp, { className: "w-5 h-5 text-gray-400" })) : (_jsx(ChevronDown, { className: "w-5 h-5 text-gray-400" }))] }), showDQPreview && (_jsx("div", { className: "mt-4 pt-4 border-t border-gray-200", children: dqExpectations.length === 0 ? (_jsx("div", { className: "text-center py-8", children: _jsx(Button, { variant: "secondary", size: "sm", onClick: handlePreviewDQ, children: "Load DQ Expectations" }) })) : (_jsx(DQExpectationsPreview, { expectations: dqExpectations })) }))] }), _jsxs(Card, { children: [_jsxs("div", { className: "flex items-center gap-2 mb-4", children: [_jsx(Code, { className: "w-5 h-5 text-primary-600" }), _jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Generate Medallion Architecture Code" })] }), _jsx("p", { className: "text-sm text-gray-600 mb-4", children: "Automatically generate complete PySpark DLT code including Bronze, Silver, and Gold layers." }), _jsx(Button, { variant: "primary", onClick: handleGenerateCode, isLoading: isGenerating, icon: _jsx(Sparkles, { className: "w-4 h-4" }), children: "Generate Medallion PySpark Code" })] }), pyspark_code && (_jsxs(Card, { children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Generated PySpark Code" }), _jsx("div", { className: "flex items-center gap-2", children: _jsx(Button, { variant: "ghost", size: "sm", onClick: () => {
                                        navigator.clipboard.writeText(pyspark_code);
                                        toast.success('Code copied to clipboard');
                                    }, children: "Copy Code" }) })] }), _jsx("div", { className: "flex gap-2 mb-4 border-b border-gray-200", children: ['full', 'bronze', 'silver', 'gold'].map((tab) => (_jsxs("button", { onClick: () => setActiveCodeTab(tab), className: `px-4 py-2 text-sm font-medium transition-colors border-b-2 ${activeCodeTab === tab
                                ? 'border-primary-600 text-primary-600'
                                : 'border-transparent text-gray-600 hover:text-gray-900'}`, children: [tab.charAt(0).toUpperCase() + tab.slice(1), " Layer"] }, tab))) }), _jsx(CodeViewer, { code: pyspark_code, language: "python" })] })), _jsx(MaskingPanel, {})] }));
};
