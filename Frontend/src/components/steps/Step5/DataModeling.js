import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState, useEffect } from 'react';
import { Database, Sparkles, Grid } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Button } from '@/components/common/Button';
import { Select } from '@/components/common/Select';
import { CodeViewer } from '@/components/steps/Step4/CodeViewer';
import { ModelStructure } from './ModelStructure';
import { ERDiagram } from './ERDiagram';
import { modelingApi } from '@/api/modeling';
import { useAgentStore } from '@/store/useAgentStore';
import toast from 'react-hot-toast';
export const DataModeling = () => {
    const { catalog, schema, modeling_sql, modeling_schema_view, modeling_er_diagram, modeling_diagram, setModelingSQL, updateState, } = useAgentStore();
    const [schemaView, setSchemaView] = useState(modeling_schema_view || 'bronze');
    const [isGenerating, setIsGenerating] = useState(false);
    const [parsedModel, setParsedModel] = useState(null);
    // Re-parse model on mount if SQL already exists in store
    useEffect(() => {
        if (modeling_sql) {
            setParsedModel(parseDimensionalModel(modeling_sql, modeling_er_diagram, modeling_diagram));
        }
    }, [modeling_sql, modeling_er_diagram, modeling_diagram]);
    const handleGenerateModel = async () => {
        if (!catalog || !schema) {
            toast.error("Session context missing. Please ensure Step 1 is complete.");
            return;
        }
        setIsGenerating(true);
        try {
            const result = (await modelingApi.generateDimensionalModel(schemaView, catalog, schema));
            if (result.data?.success) {
                toast.success('Dimensional model generated successfully');
                const sql = result.data.modeling_sql || '';
                const erDiagram = result.data.er_diagram || { tables: [], relationships: [] };
                const erGraph = result.data.diagram || { nodes: [], edges: [] };
                setModelingSQL(sql);
                updateState({
                    modeling_schema_view: schemaView,
                    modeling_er_diagram: erDiagram,
                    modeling_diagram: erGraph,
                });
                setParsedModel(parseDimensionalModel(sql, erDiagram, erGraph));
            }
            else {
                toast.error(result.data?.message || result.error || 'Failed to generate model');
            }
        }
        catch (error) {
            console.error("Modeling Error:", error);
            const errorMsg = error.response?.data?.detail?.[0]?.msg || "Server communication error";
            toast.error(errorMsg);
        }
        finally {
            setIsGenerating(false);
        }
    };
    return (_jsxs("div", { className: "space-y-6", children: [_jsxs("div", { children: [_jsx("h2", { className: "text-2xl font-bold text-gray-900 mb-2", children: "Step 2: Dimensional Data Modeling" }), _jsx("p", { className: "text-gray-600", children: "Generate a Kimball-style dimensional model optimized for analytics." })] }), _jsxs(Card, { children: [_jsxs("div", { className: "flex items-center gap-2 mb-4", children: [_jsx(Database, { className: "w-5 h-5 text-primary-600" }), _jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Model Configuration" })] }), _jsxs("div", { className: "space-y-4", children: [_jsx(Select, { label: "Select Schema to Model", options: [
                                    { value: 'bronze', label: 'Bronze (Raw Data - Recommended for Step 2)' },
                                    { value: 'silver', label: 'Silver (Requires Step 3 mappings)' },
                                ], value: schemaView, onChange: (e) => setSchemaView(e.target.value) }), _jsx(Button, { variant: "primary", onClick: handleGenerateModel, isLoading: isGenerating, icon: _jsx(Sparkles, { className: "w-4 h-4" }), children: "Generate Dimensional Model SQL" })] })] }), modeling_sql && (_jsx(ERDiagram, { sql: modeling_sql, diagram: modeling_er_diagram, graph: modeling_diagram })), parsedModel && (_jsxs(Card, { children: [_jsxs("div", { className: "flex items-center gap-2 mb-4", children: [_jsx(Grid, { className: "w-5 h-5 text-success-600" }), _jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Structure Overview" })] }), _jsx(ModelStructure, { dimensions: parsedModel.dimensions, facts: parsedModel.facts })] })), modeling_sql && (_jsxs(Card, { children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "SQL DDL" }), _jsx(Button, { variant: "ghost", size: "sm", onClick: () => {
                                    navigator.clipboard.writeText(modeling_sql);
                                    toast.success('SQL copied');
                                }, children: "Copy SQL" })] }), _jsx(CodeViewer, { code: modeling_sql, language: "sql" })] }))] }));
};
// Helper parser logic
function parseDimensionalModel(sql, diagram, graph) {
    const dimensions = [];
    const facts = [];
    const seen = new Set();
    const addTable = (tableName, type) => {
        if (!tableName)
            return;
        const normalized = tableName.toLowerCase();
        if (seen.has(normalized))
            return;
        seen.add(normalized);
        if (type === 'dimension' || normalized.startsWith('dim_'))
            dimensions.push(normalized);
        else if (type === 'fact' || normalized.startsWith('fact_'))
            facts.push(normalized);
    };
    if (graph?.nodes?.length) {
        graph.nodes.forEach((node) => addTable(node.table_name, node.table_type));
        return { dimensions, facts };
    }
    if (diagram?.tables?.length) {
        diagram.tables.forEach((table) => {
            addTable(table.name);
        });
        return { dimensions, facts };
    }
    const tableRegex = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/gi;
    let match;
    while ((match = tableRegex.exec(sql)) !== null) {
        const name = match[1].toLowerCase();
        addTable(name);
    }
    return { dimensions, facts };
}
