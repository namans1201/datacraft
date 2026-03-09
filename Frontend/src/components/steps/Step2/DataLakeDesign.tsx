import React, { useState } from 'react';
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

export const DataLakeDesign: React.FC = () => {
  const {
    catalog,         
    schema,
    dbfs_path,       // Ensure this is retrieved from store
    mapping_rows,
    gold_mapping_rows,
    setMappings,
    setGoldMappings,
  } = useAgentStore();

  const [standard, setStandard] = useState('fhir');
  const [customFile, setCustomFile] = useState<File | null>(null);
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
      const response = await mappingApi.generateSilverMappings(
        standard,
        catalog,
        schema,
        dbfs_path, 
        customFile || undefined
      );

      // Handle Axios response wrapping
      const result = (response as any).data || response;

      if (result.success) {
        toast.success('Silver mappings generated!');
        setMappings(result.mapping_rows || []);
      } else {
        toast.error(result.message || 'Failed to generate silver mappings');
      }
    } catch (error: any) {
      // This captures the 404 "State not found" from the backend
      const detail = error.response?.data?.detail || "Mapping error";
      toast.error(typeof detail === 'string' ? detail : "Internal Server Error");
    } finally {
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
      
      const result = (response as any).data || response;

      if (result.success) {
        toast.success('Gold mappings generated successfully');
        setGoldMappings(result.gold_mapping_rows || []);
      } else {
        toast.error(result.message || 'Failed to generate gold mappings');
      }
    } catch (error: any) {
      console.error("Gold Mapping Error:", error);
      toast.error(error.response?.data?.detail || "Error generating gold mappings");
    } finally {
      setIsGeneratingGold(false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900 mb-2">
          Step 3: Data Lake Design
        </h2>
        <p className="text-gray-600">
          Map your raw data to standardized schemas using industry standards or custom mappings.
        </p>
      </div>

      {/* Mapping Standard Selection */}
      <Card>
        <div className="flex items-center gap-2 mb-4">
          <Layers className="w-5 h-5 text-primary-600" />
          <h3 className="text-lg font-semibold text-gray-900">
            Bronze → Silver Mapping
          </h3>
        </div>

        <div className="space-y-4">
          <Select
            label="Select Mapping Standard"
            options={mappingStandards}
            value={standard}
            onChange={(e) => setStandard(e.target.value)}
          />

          {standard === 'custom' && (
            <CustomSchemaUploader
              onFileSelect={setCustomFile}
              selectedFile={customFile}
            />
          )}

          <Button
            variant="primary"
            onClick={handleGenerateSilverMappings}
            isLoading={isGeneratingSilver}
            icon={<Sparkles className="w-4 h-4" />}
          >
            Generate Silver Mappings
          </Button>
        </div>
      </Card>

      {/* Silver Mappings Table */}
      {mapping_rows.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900">
              Bronze → Silver Mappings
            </h3>
            <span className="text-sm text-gray-500">
              {mapping_rows.length} mappings
            </span>
          </div>
          <SilverMappingTable mappings={mapping_rows} />
        </Card>
      )}

      {/* Gold Mapping Generation */}
      {mapping_rows.length > 0 && (
        <>
          <div className="flex items-center justify-center py-4">
            <ArrowRight className="w-6 h-6 text-gray-400" />
          </div>

          <Card>
            <div className="flex items-center gap-2 mb-4">
              <Layers className="w-5 h-5 text-success-600" />
              <h3 className="text-lg font-semibold text-gray-900">
                Silver → Gold Mapping
              </h3>
            </div>

            <p className="text-sm text-gray-600 mb-4">
              Create analytical gold layer tables with aggregated metrics and dimensional models.
            </p>

            <Button
              variant="primary"
              onClick={handleGenerateGoldMappings}
              isLoading={isGeneratingGold}
              icon={<Sparkles className="w-4 h-4" />}
            >
              Generate Gold Mappings
            </Button>
          </Card>
        </>
      )}

      {/* Gold Mappings Table */}
      {gold_mapping_rows.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900">
              Silver → Gold Mappings
            </h3>
            <span className="text-sm text-gray-500">
              {gold_mapping_rows.length} mappings
            </span>
          </div>
          <GoldMappingTable mappings={gold_mapping_rows} />
        </Card>
      )}
    </div>
  );
};
