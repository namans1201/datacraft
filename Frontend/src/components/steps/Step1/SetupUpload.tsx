import React, { useState } from 'react';
import { Upload, Database, FolderOpen } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Input } from '@/components/common/Input';
import { Button } from '@/components/common/Button';
import { FileUploader } from './FileUploader';
import { FilePreview } from './FilePreview';
import { databricksApi } from '@/api/databricks';
import { useAgentStore } from '@/store/useAgentStore';
import toast from 'react-hot-toast';

export const SetupUpload: React.FC = () => {
  const {
    catalog,
    schema,
    volume,
    setCatalogInfo,
    df_heads,
    updateState, 
  } = useAgentStore();

  const [form, setForm] = useState({
    catalog: catalog || '',
    schema: schema || '',
    volume: volume || '',
    token: '',
  });

  const [isCreating, setIsCreating] = useState(false);
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);

  const handleCreateCatalog = async () => {
    if (!form.catalog || !form.schema || !form.volume || !form.token) {
      toast.error('Please fill all fields');
      return;
    }

    setIsCreating(true);
    try {
      const catalogResult = await databricksApi.createCatalog({
        catalog: form.catalog,
        token: form.token,
      });

      if (!catalogResult.success) {
        toast.error(catalogResult.error || 'Failed to create catalog');
        return;
      }

      const schemaResult = await databricksApi.createSchema({
        catalog: form.catalog,
        schema_name: form.schema,
        token: form.token,
      });

      if (!schemaResult.success) {
        toast.error(schemaResult.error || 'Failed to create schema');
        return;
      }

      const volumeResult = await databricksApi.createVolume({
        catalog: form.catalog,
        schema_name: form.schema,
        volume: form.volume,
        token: form.token,
      });

      if (volumeResult.success) {
        toast.success('Catalog, Schema, and Volume created successfully');
        // Update store with form values
        setCatalogInfo(form.catalog, form.schema, form.volume);
        updateState({ schema: form.schema });
      } else {
        toast.error(volumeResult.error || 'Failed to create volume');
      }
    } catch (err) {
      toast.error('An error occurred during setup');
    } finally {
      setIsCreating(false);
    }
  };

  const handleFileSelect = (file: File) => {
    setUploadedFile(file);
  };

  const handleFileUpload = async () => {
      if (!uploadedFile) return;
      if (!form.catalog || !form.schema || !form.token) {
        toast.error('Catalog, Schema, and Token are required');
        return;
      }

      setIsUploading(true);
      const loadingToast = toast.loading('Step 1: Uploading...');

      try {
        setCatalogInfo(form.catalog, form.schema, form.volume);
        updateState({ schema: form.schema });

        const uploadRes = (await databricksApi.uploadFile(
          uploadedFile, form.catalog, form.schema, form.volume, form.token
        )) as any;

        const path = uploadRes?.data?.dbfs_path || uploadRes?.dbfs_path;

        if (path) {
          toast.loading('Processing and profiling uploaded files...', { id: loadingToast });
          
          const readRes = (await databricksApi.readFiles(
            path, 
            form.token,   
            form.catalog, 
            form.schema   
          )) as any;

          console.log("Read API Raw Response:", readRes);
          
          const actualState = readRes?.data?.data?.data; 

          if (actualState && actualState.df_heads) {
            (updateState as any)({
              dbfs_path: actualState.dbfs_path,
              df_heads: actualState.df_heads,
              df_dtypes: actualState.df_dtypes || {},
              pii_columns: actualState.pii_columns || [],
              phi_columns: actualState.phi_columns || [],
              sensitive_metadata: actualState.sensitive_metadata || {},
            });
            
            toast.success('Preview loaded successfully!', { id: loadingToast });
          } else {
          // This logs if the path we used above (readRes.data.data.data) was wrong
          console.log("ActualState check failed. actualState is:", actualState);
          toast.error("Data found but path mapping failed. Check console.", { id: loadingToast });
        }
        }
      } catch (err) {
        console.error("Critical Error:", err);
        toast.error('Network error during processing', { id: loadingToast });
      } finally {
        setIsUploading(false);
      }
    };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900 mb-2">
          Step 1: Catalog Setup & File Upload
        </h2>
        <p className="text-gray-600">
          Create or specify your Databricks catalog, schema, and volume, then upload your data file.
        </p>
      </div>

      <Card>
        <div className="flex items-center gap-2 mb-4">
          <Database className="w-5 h-5 text-primary-600" />
          <h3 className="text-lg font-semibold text-gray-900">Databricks Configuration</h3>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <Input
            label="Catalog Name"
            placeholder="Enter catalog name"
            value={form.catalog}
            onChange={(e) => setForm({ ...form, catalog: e.target.value })}
          />
          <Input
            label="Schema Name"
            placeholder="Enter schema name"
            value={form.schema}
            onChange={(e) => setForm({ ...form, schema: e.target.value })}
          />
          <Input
            label="Volume Name"
            placeholder="Enter volume name"
            value={form.volume}
            onChange={(e) => setForm({ ...form, volume: e.target.value })}
          />
          <Input
            label="Databricks Access Token"
            type="password"
            placeholder="Enter your access token"
            value={form.token}
            onChange={(e) => setForm({ ...form, token: e.target.value })}
          />
        </div>

        <div className="mt-6">
          <Button
            variant="primary"
            onClick={handleCreateCatalog}
            isLoading={isCreating}
            icon={<FolderOpen className="w-4 h-4" />}
          >
            Create Catalog, Schema & Volume
          </Button>
        </div>
      </Card>

      <Card>
        <div className="flex items-center gap-2 mb-4">
          <Upload className="w-5 h-5 text-primary-600" />
          <h3 className="text-lg font-semibold text-gray-900">Upload Data File</h3>
        </div>

        <FileUploader onFileSelect={handleFileSelect} />

        {uploadedFile && (
          <div className="mt-4 flex items-center justify-between p-4 bg-gray-50 rounded-lg">
            <div>
              <p className="text-sm font-medium text-gray-900">{uploadedFile.name}</p>
              <p className="text-xs text-gray-500">
                {(uploadedFile.size / 1024 / 1024).toFixed(2)} MB
              </p>
            </div>
            <Button
              variant="primary"
              size="sm"
              onClick={handleFileUpload}
              isLoading={isUploading}
            >
              Upload to Databricks
            </Button>
          </div>
        )}
      </Card>

      {/* Data Preview Section */}
      {df_heads && Object.keys(df_heads).length > 0 && (
        <div className="space-y-8">
          {Object.entries(df_heads).map(([name, head]: [string, any]) => (
            <Card key={name} className="p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">
                {name} — Data Preview
              </h3>
              <FilePreview 
                fileName={name} 
                data={head} 
              />
            </Card>
          ))}
        </div>
      )}
    </div>
  );
};
