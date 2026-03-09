import React, { useState, useEffect } from 'react';
import { TrendingUp, Target, Sparkles } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Button } from '@/components/common/Button';
import { Select } from '@/components/common/Select';
import { KPICard } from './KPICard';
import { kpiApi } from '@/api/kpi';
import { useAgentStore } from '@/store/useAgentStore';
import toast from 'react-hot-toast';

export const BusinessKPIs: React.FC = () => {
  const {
    catalog,   
    schema,
    domain,
    area,
    suggested_areas,
    kpis,
    setKPIs,
    updateState,
  } = useAgentStore();

  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [isGenerating, setIsGenerating] = useState(false);
  const [selectedArea, setSelectedArea] = useState(area || '');
  const [parsedKPIs, setParsedKPIs] = useState<any[]>([]);

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
      const response = (await kpiApi.analyzeSchema(catalog, schema)) as any;
      
      // Some apiClients return the data directly, others wrap it in .data
      const result = response.data || response;
      console.log("Backend Response:", result);

      if (result.success) {
        toast.success('Schema analyzed successfully');
        
        updateState({
          domain: result.domain || "Unknown",
          suggested_areas: result.suggested_areas || [],
        });
      } else {
        toast.error(result.message || 'Failed to analyze schema');
      }
    } catch (error: any) {
      console.error("API Error:", error);
      toast.error(error.response?.data?.detail || "Error during analysis");
    } finally {
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
      const response = (await kpiApi.generateKPIs(domain, selectedArea, catalog, schema)) as any;
      const result = response.data || response;

      if (result.success) {
        toast.success('KPIs generated successfully');
        setKPIs(domain, selectedArea, result.kpis || '');
      } else {
        toast.error(result.message || 'Failed to generate KPIs');
      }
    } catch (error: any) {
      toast.error(error.response?.data?.detail || "Error generating KPIs");
    } finally {
      setIsGenerating(false);
    }
  };

  const areaOptions = suggested_areas.map(a => ({ value: a, label: a }));

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Step 4: Business KPIs</h2>
        <p className="text-gray-600">Detect business domain and generate relevant KPIs.</p>
      </div>

      <Card>
        <div className="flex items-center gap-2 mb-4">
          <Target className="w-5 h-5 text-primary-600" />
          <h3 className="text-lg font-semibold text-gray-900">Domain Detection</h3>
        </div>

        <Button
          variant="primary"
          onClick={handleAnalyzeSchema}
          isLoading={isAnalyzing}
          icon={<Sparkles className="w-4 h-4" />}
        >
          Analyze Schema & Detect Domain
        </Button>

        {domain && (
          <div className="mt-4 p-4 bg-primary-50 border-l-4 border-primary-500 rounded-r-lg">
            <div className="flex items-start gap-3">
              <TrendingUp className="w-5 h-5 text-primary-600 mt-0.5" />
              <div>
                <p className="text-sm font-semibold text-gray-900">
                  Detected Domain: <span className="text-primary-700">{domain}</span>
                </p>
              </div>
            </div>
          </div>
        )}
      </Card>

      {domain && suggested_areas.length > 0 && (
        <Card>
          <div className="flex items-center gap-2 mb-4">
            <TrendingUp className="w-5 h-5 text-success-600" />
            <h3 className="text-lg font-semibold text-gray-900">KPI Generation</h3>
          </div>
          <div className="space-y-4">
            <Select
              label="Select Area of Interest"
              options={[{ value: '', label: 'Choose an area...' }, ...areaOptions]}
              value={selectedArea}
              onChange={(e) => setSelectedArea(e.target.value)}
            />
            <Button
              variant="primary"
              onClick={handleGenerateKPIs}
              isLoading={isGenerating}
              disabled={!selectedArea}
              icon={<Sparkles className="w-4 h-4" />}
            >
              Generate KPIs
            </Button>
          </div>
        </Card>
      )}

      {parsedKPIs.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900">Generated KPIs</h3>
            <span className="text-sm text-gray-500">{parsedKPIs.length} KPIs</span>
          </div>
          <div className="space-y-4">
            {parsedKPIs.map((kpi, index) => (
              <KPICard key={index} kpi={kpi} />
            ))}
          </div>
        </Card>
      )}
    </div>
  );
};

function parseKPIsFromText(text: string): any[] {
  const lines = text.split('\n').filter(l => l.trim());
  const kpis: any[] = [];
  let currentKPI: any = null;
  for (const line of lines) {
    const kpiMatch = line.match(/^(.+?)\s*=\s*(.+)$/);
    if (kpiMatch) {
      if (currentKPI) kpis.push(currentKPI);
      currentKPI = { name: kpiMatch[1].trim(), formula: kpiMatch[2].trim(), description: '', business_context: '' };
    } else if (currentKPI && line.startsWith('--')) {
      const comment = line.replace(/^--\s*/, '').trim();
      if (!currentKPI.description) currentKPI.description = comment;
      else currentKPI.business_context = comment;
    }
  }
  if (currentKPI) kpis.push(currentKPI);
  return kpis.slice(0, 10);
}
