import React, { useState } from 'react';
import { Code, Shield, Sparkles, ChevronDown, ChevronUp } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Button } from '@/components/common/Button';
import { DQExpectationsPreview } from './DQExpectationsPreview';
import { CodeViewer } from './CodeViewer';
import { MaskingPanel } from './MaskingPanel';
import { codegenApi } from '@/api/codegen';
import { useAgentStore } from '@/store/useAgentStore';
import toast from 'react-hot-toast';

export const CodeGeneration: React.FC = () => {
  const {
    catalog,
    schema_name,
    pyspark_code,
    dq_rules,
    masking_sql,
    setPySparkCode,
    setMaskingSQL,
    updateState,
  } = useAgentStore() as any; 

  const [isGenerating, setIsGenerating] = useState(false);
  const [showDQPreview, setShowDQPreview] = useState(false);
  const [dqExpectations, setDqExpectations] = useState<any[]>([]);
  const [activeCodeTab, setActiveCodeTab] = useState<'bronze' | 'silver' | 'gold' | 'full'>('full');

  const handlePreviewDQ = async () => {
    if (!catalog || !schema_name) {
      toast.error("Catalog and Schema are missing. Please re-run 'Read Files'.");
      return;
    }
    try {
      const response = await codegenApi.previewDQExpectations(catalog, schema_name);
      
      const data = (response.data || []) as any[];
      setDqExpectations(data);
      setShowDQPreview(true);
    } catch (error: any) {
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
      const result = response.data as { 
        success: boolean; 
        pyspark_code?: string; 
        message?: string 
      };
      
      if (result && result.success) {
        toast.success('Medallion code generated successfully');
        setPySparkCode(result.pyspark_code || '');
        updateState({ dq_rules: result.message || '' });
      } else {
        toast.error(result?.message || 'Failed to generate code');
      }
    } catch (error: any) {
      toast.error(error.response?.data?.detail || 'Failed to generate code');
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900 mb-2">
          Step 5: Medallion PySpark Code Generation
        </h2>
        <p className="text-gray-600">
          Generate production-ready PySpark Delta Live Tables code implementing the Medallion architecture.
        </p>
      </div>

      {/* DQ Expectations Preview */}
      <Card>
        <button
          onClick={() => setShowDQPreview(!showDQPreview)}
          className="w-full flex items-center justify-between text-left"
        >
          <div className="flex items-center gap-2">
            <Shield className="w-5 h-5 text-primary-600" />
            <h3 className="text-lg font-semibold text-gray-900">
              Preview Data Quality Expectations
            </h3>
          </div>
          {showDQPreview ? (
            <ChevronUp className="w-5 h-5 text-gray-400" />
          ) : (
            <ChevronDown className="w-5 h-5 text-gray-400" />
          )}
        </button>

        {showDQPreview && (
          <div className="mt-4 pt-4 border-t border-gray-200">
            {dqExpectations.length === 0 ? (
              <div className="text-center py-8">
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={handlePreviewDQ}
                >
                  Load DQ Expectations
                </Button>
              </div>
            ) : (
              <DQExpectationsPreview expectations={dqExpectations} />
            )}
          </div>
        )}
      </Card>

      {/* Code Generation Section */}
      <Card>
        <div className="flex items-center gap-2 mb-4">
          <Code className="w-5 h-5 text-primary-600" />
          <h3 className="text-lg font-semibold text-gray-900">
            Generate Medallion Architecture Code
          </h3>
        </div>

        <p className="text-sm text-gray-600 mb-4">
          Automatically generate complete PySpark DLT code including Bronze, Silver, and Gold layers.
        </p>

        <Button
          variant="primary"
          onClick={handleGenerateCode}
          isLoading={isGenerating}
          icon={<Sparkles className="w-4 h-4" />}
        >
          Generate Medallion PySpark Code
        </Button>
      </Card>

      {/* Generated Code Display */}
      {pyspark_code && (
        <Card>
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900">
              Generated PySpark Code
            </h3>
            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                size="sm"
                onClick={() => {
                  navigator.clipboard.writeText(pyspark_code);
                  toast.success('Code copied to clipboard');
                }}
              >
                Copy Code
              </Button>
            </div>
          </div>

          <div className="flex gap-2 mb-4 border-b border-gray-200">
            {['full', 'bronze', 'silver', 'gold'].map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveCodeTab(tab as any)}
                className={`px-4 py-2 text-sm font-medium transition-colors border-b-2 ${
                  activeCodeTab === tab
                    ? 'border-primary-600 text-primary-600'
                    : 'border-transparent text-gray-600 hover:text-gray-900'
                }`}
              >
                {tab.charAt(0).toUpperCase() + tab.slice(1)} Layer
              </button>
            ))}
          </div>

          <CodeViewer code={pyspark_code} language="python" />
        </Card>
      )}

      {/* Masking Panel Section */}
      <MaskingPanel />
    </div>
  );
};
