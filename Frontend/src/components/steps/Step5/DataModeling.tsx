import React, { useState, useEffect } from 'react';
import { Database, Sparkles, Grid, MessageSquare } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Button } from '@/components/common/Button';
import { Select } from '@/components/common/Select';
import { CodeViewer } from '@/components/steps/Step4/CodeViewer';
import { ModelStructure } from './ModelStructure';
import { ERDiagram } from './ERDiagram'; 
import { modelingApi } from '@/api/modeling';
import { useAgentStore } from '@/store/useAgentStore';
import { ApiResponse } from '@/types/api';
import { ERDiagram as ERDiagramType, ERDiagramGraph } from '@/types/agent-state';
import toast from 'react-hot-toast';

interface ModelingResponse {
  success: boolean;
  modeling_sql?: string;
  er_diagram?: ERDiagramType;
  diagram?: ERDiagramGraph;
  message?: string;
}

export const DataModeling: React.FC = () => {
  const {
    catalog,         
    schema,     
    modeling_sql,
    modeling_schema_view,
    modeling_er_diagram,
    modeling_diagram,
    setModelingSQL,
    updateState,
  } = useAgentStore();

  const [schemaView, setSchemaView] = useState<'bronze' | 'silver'>(
    (modeling_schema_view as 'bronze' | 'silver') || 'bronze'
  );
  const [isGenerating, setIsGenerating] = useState(false);
  const [parsedModel, setParsedModel] = useState<{
    dimensions: string[];
    facts: string[];
  } | null>(null);

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
      const result = (await modelingApi.generateDimensionalModel(
        schemaView, 
        catalog, 
        schema
      )) as ApiResponse<ModelingResponse>;

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
      } else {
        toast.error(result.data?.message || result.error || 'Failed to generate model');
      }
    } catch (error: any) {
      console.error("Modeling Error:", error);
      const errorMsg = error.response?.data?.detail?.[0]?.msg || "Server communication error";
      toast.error(errorMsg);
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Step 2: Dimensional Data Modeling</h2>
        <p className="text-gray-600">Generate a Kimball-style dimensional model optimized for analytics.</p>
      </div>

      <Card>
        <div className="flex items-center gap-2 mb-4">
          <Database className="w-5 h-5 text-primary-600" />
          <h3 className="text-lg font-semibold text-gray-900">Model Configuration</h3>
        </div>
        <div className="space-y-4">
          <Select
            label="Select Schema to Model"
            options={[
              { value: 'bronze', label: 'Bronze (Raw Data - Recommended for Step 2)' },
              { value: 'silver', label: 'Silver (Requires Step 3 mappings)' },
            ]}
            value={schemaView}
            onChange={(e) => setSchemaView(e.target.value as 'bronze' | 'silver')}
          />
          <Button
            variant="primary"
            onClick={handleGenerateModel}
            isLoading={isGenerating}
            icon={<Sparkles className="w-4 h-4" />}
          >
            Generate Dimensional Model SQL
          </Button>
        </div>
      </Card>

      {/* ER Diagram Visualization */}
      {modeling_sql && (
        <ERDiagram sql={modeling_sql} diagram={modeling_er_diagram} graph={modeling_diagram} />
      )}

      {/* Model Structure Lists */}
      {parsedModel && (
        <Card>
          <div className="flex items-center gap-2 mb-4">
            <Grid className="w-5 h-5 text-success-600" />
            <h3 className="text-lg font-semibold text-gray-900">Structure Overview</h3>
          </div>
          <ModelStructure 
            dimensions={parsedModel.dimensions} 
            facts={parsedModel.facts} 
          />
        </Card>
      )}

      {/* SQL DDL Viewer */}
      {modeling_sql && (
        <Card>
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900">SQL DDL</h3>
            <Button variant="ghost" size="sm" onClick={() => {
              navigator.clipboard.writeText(modeling_sql);
              toast.success('SQL copied');
            }}>Copy SQL</Button>
          </div>
          <CodeViewer code={modeling_sql} language="sql" />
        </Card>
      )}
    </div>
  );
};

// Helper parser logic
function parseDimensionalModel(sql: string, diagram?: ERDiagramType, graph?: ERDiagramGraph) {
  const dimensions: string[] = [];
  const facts: string[] = [];
  const seen: Set<string> = new Set();

  const addTable = (tableName?: string, type?: string) => {
    if (!tableName) return;
    const normalized = tableName.toLowerCase();
    if (seen.has(normalized)) return;
    seen.add(normalized);
    if (type === 'dimension' || normalized.startsWith('dim_')) dimensions.push(normalized);
    else if (type === 'fact' || normalized.startsWith('fact_')) facts.push(normalized);
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
