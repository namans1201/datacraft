import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState, useEffect } from 'react';
import { TrendingUp, Target, Sparkles } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Button } from '@/components/common/Button';
import { Select } from '@/components/common/Select';
import { KPICard } from './KPICard';
import { kpiApi } from '@/api/kpi';
import { useAgentStore } from '@/store/useAgentStore';
import toast from 'react-hot-toast';
export const BusinessKPIs = () => {
    const { catalog, schema, domain, area, suggested_areas, kpis, setKPIs, updateState, } = useAgentStore();
    const [isAnalyzing, setIsAnalyzing] = useState(false);
    const [isGenerating, setIsGenerating] = useState(false);
    const [selectedArea, setSelectedArea] = useState(area || '');
    const [parsedKPIs, setParsedKPIs] = useState([]);
    useEffect(() => {
        if (kpis) {
            const kpiList = parseKPIsFromText(kpis);
            setParsedKPIs(kpiList);
        }
    }, [kpis]);
    const handleAnalyzeSchema = async () => {
        if (!catalog || !schema) {
            toast.error("Required context (Catalog or Schema) is missing.");
            return;
        }
        setIsAnalyzing(true);
        try {
            const response = (await kpiApi.analyzeSchema(catalog, schema));
            // Some apiClients return the data directly, others wrap it in .data
            const result = response.data || response;
            console.log("Backend Response:", result);
            if (result.success) {
                toast.success('Schema analyzed successfully');
                updateState({
                    domain: result.domain || "Unknown",
                    suggested_areas: result.suggested_areas || [],
                });
            }
            else {
                toast.error(result.message || 'Failed to analyze schema');
            }
        }
        catch (error) {
            console.error("API Error:", error);
            toast.error(error.response?.data?.detail || "Error during analysis");
        }
        finally {
            setIsAnalyzing(false);
        }
    };
    const handleGenerateKPIs = async () => {
        if (!domain || !selectedArea || !catalog || !schema) {
            toast.error('Please analyze schema and select an area first');
            return;
        }
        setIsGenerating(true);
        try {
            const response = (await kpiApi.generateKPIs(domain, selectedArea, catalog, schema));
            const result = response.data || response;
            if (result.success) {
                toast.success('KPIs generated successfully');
                setKPIs(domain, selectedArea, result.kpis || '');
            }
            else {
                toast.error(result.message || 'Failed to generate KPIs');
            }
        }
        catch (error) {
            toast.error(error.response?.data?.detail || "Error generating KPIs");
        }
        finally {
            setIsGenerating(false);
        }
    };
    const areaOptions = suggested_areas.map(a => ({ value: a, label: a }));
    return (_jsxs("div", { className: "space-y-6", children: [_jsxs("div", { children: [_jsx("h2", { className: "text-2xl font-bold text-gray-900 mb-2", children: "Step 4: Business KPIs" }), _jsx("p", { className: "text-gray-600", children: "Detect business domain and generate relevant KPIs." })] }), _jsxs(Card, { children: [_jsxs("div", { className: "flex items-center gap-2 mb-4", children: [_jsx(Target, { className: "w-5 h-5 text-primary-600" }), _jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Domain Detection" })] }), _jsx(Button, { variant: "primary", onClick: handleAnalyzeSchema, isLoading: isAnalyzing, icon: _jsx(Sparkles, { className: "w-4 h-4" }), children: "Analyze Schema & Detect Domain" }), domain && (_jsx("div", { className: "mt-4 p-4 bg-primary-50 border-l-4 border-primary-500 rounded-r-lg", children: _jsxs("div", { className: "flex items-start gap-3", children: [_jsx(TrendingUp, { className: "w-5 h-5 text-primary-600 mt-0.5" }), _jsx("div", { children: _jsxs("p", { className: "text-sm font-semibold text-gray-900", children: ["Detected Domain: ", _jsx("span", { className: "text-primary-700", children: domain })] }) })] }) }))] }), domain && suggested_areas.length > 0 && (_jsxs(Card, { children: [_jsxs("div", { className: "flex items-center gap-2 mb-4", children: [_jsx(TrendingUp, { className: "w-5 h-5 text-success-600" }), _jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "KPI Generation" })] }), _jsxs("div", { className: "space-y-4", children: [_jsx(Select, { label: "Select Area of Interest", options: [{ value: '', label: 'Choose an area...' }, ...areaOptions], value: selectedArea, onChange: (e) => setSelectedArea(e.target.value) }), _jsx(Button, { variant: "primary", onClick: handleGenerateKPIs, isLoading: isGenerating, disabled: !selectedArea, icon: _jsx(Sparkles, { className: "w-4 h-4" }), children: "Generate KPIs" })] })] })), parsedKPIs.length > 0 && (_jsxs(Card, { children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Generated KPIs" }), _jsxs("span", { className: "text-sm text-gray-500", children: [parsedKPIs.length, " KPIs"] })] }), _jsx("div", { className: "space-y-4", children: parsedKPIs.map((kpi, index) => (_jsx(KPICard, { kpi: kpi }, index))) })] }))] }));
};
function parseKPIsFromText(text) {
    const lines = text.split('\n').filter(l => l.trim());
    const kpis = [];
    let currentKPI = null;
    for (const line of lines) {
        const kpiMatch = line.match(/^(.+?)\s*=\s*(.+)$/);
        if (kpiMatch) {
            if (currentKPI)
                kpis.push(currentKPI);
            currentKPI = { name: kpiMatch[1].trim(), formula: kpiMatch[2].trim(), description: '', business_context: '' };
        }
        else if (currentKPI && line.startsWith('--')) {
            const comment = line.replace(/^--\s*/, '').trim();
            if (!currentKPI.description)
                currentKPI.description = comment;
            else
                currentKPI.business_context = comment;
        }
    }
    if (currentKPI)
        kpis.push(currentKPI);
    return kpis.slice(0, 10);
}
