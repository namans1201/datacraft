import React, { useState } from 'react';
import { Shield, Lock, Users, User, Play } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Button } from '@/components/common/Button';
import { Select } from '@/components/common/Select';
import { Input } from '@/components/common/Input';
import { CodeViewer } from './CodeViewer';
import { ExecutionLog } from './ExecutionLog';
import { codegenApi } from '@/api/codegen';
import { useAgentStore } from '@/store/useAgentStore';
import { ApiResponse, GenerateCodeRequest } from '@/types/api';
import toast from 'react-hot-toast';

interface MaskingConfig {
  pii_access_mode: 'group' | 'user';
  pii_access_value: string;
  phi_access_mode: 'group' | 'user';
  phi_access_value: string;
}

interface MaskingSqlResponse {
  masking_sql?: string;
}

interface ExecuteMaskingResponse {
  execution_log?: Array<string | { timestamp?: string; level?: 'info' | 'warning' | 'error'; message?: string }>;
}

export const MaskingPanel: React.FC = () => {
  const {
    catalog,      
    schema,
    masking_sql,
    mask_execution_status,
    mask_execution_log,
    pii_access_mode,
    pii_access_value,
    phi_access_mode,
    phi_access_value,
    setMaskingSQL,
    updateState,
  } = useAgentStore();

  const [localConfig, setLocalConfig] = useState<MaskingConfig>({
    pii_access_mode: pii_access_mode ?? 'group',
    pii_access_value: pii_access_value || 'pii_access',
    phi_access_mode: phi_access_mode ?? 'group',
    phi_access_value: phi_access_value || 'phi_access',
  });

  const [isGenerating, setIsGenerating] = useState(false);
  const [isExecuting, setIsExecuting] = useState(false);

  const handleGenerateMaskingSQL = async () => {
    setIsGenerating(true);

    const request: GenerateCodeRequest = {
      include_masking: true,
      catalog: catalog,
      schema_name: schema,
      pii_access_mode: localConfig.pii_access_mode,
      pii_access_value: localConfig.pii_access_value,
      phi_access_mode: localConfig.phi_access_mode,
      phi_access_value: localConfig.phi_access_value,
    };
    const result = (await codegenApi.generateMaskingSQL(request)) as ApiResponse<MaskingSqlResponse>;

    setIsGenerating(false);

    if (result.success) {
      toast.success('Masking SQL generated successfully');
      setMaskingSQL(result.data?.masking_sql || '');
      updateState({
        pii_access_mode: localConfig.pii_access_mode,
        pii_access_value: localConfig.pii_access_value,
        phi_access_mode: localConfig.phi_access_mode,
        phi_access_value: localConfig.phi_access_value,
      }); 
    } else {
      toast.error(result.error || 'Failed to generate masking SQL');
    }
  };

  const handleExecuteMasking = async () => {
    if (!masking_sql) {
      toast.error('Please generate masking SQL first');
      return;
    }

    setIsExecuting(true);
    updateState({ mask_execution_status: 'RUNNING' });

    const result = (await codegenApi.executeMaskingSQL(catalog, schema)) as ApiResponse<ExecuteMaskingResponse>;

    setIsExecuting(false);

    if (result.success) {
      toast.success('Masking SQL executed successfully');
      updateState({ 
        mask_execution_status: 'SUCCESS',
        mask_execution_log: (result.data?.execution_log || []).map((entry) =>
          typeof entry === 'string'
            ? {
                timestamp: new Date().toISOString(),
                level: 'info',
                message: entry,
              }
            : {
                timestamp: entry.timestamp || new Date().toISOString(),
                level: entry.level || 'info',
                message: entry.message || '',
              }
        ),
      });
    } else {
      toast.error(result.error || 'Failed to execute masking SQL');
      updateState({ mask_execution_status: 'FAILED' });
    }
  };

  return (
    <>
      <Card>
        <div className="flex items-center gap-2 mb-4">
          <Shield className="w-5 h-5 text-warning-600" />
          <h3 className="text-lg font-semibold text-gray-900">
            Access Control & Data Masking
          </h3>
        </div>

        <p className="text-sm text-gray-600 mb-6">
          Configure access control policies and generate masking SQL functions for PII and PHI data protection.
        </p>

        <div className="space-y-6">
          {/* PII Access Control */}
          <div className="p-4 bg-danger-50 border border-danger-200 rounded-lg">
            <div className="flex items-center gap-2 mb-3">
              <Lock className="w-4 h-4 text-danger-600" />
              <h4 className="text-sm font-semibold text-gray-900">PII Data Access</h4>
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <Select
                label="Access Control Type"
                options={[
                  { value: 'group', label: 'Allow by Group' },
                  { value: 'user', label: 'Allow by User' },
                ]}
                value={localConfig.pii_access_mode}
                onChange={(e) =>
                  setLocalConfig({
                    ...localConfig,
                    pii_access_mode: e.target.value as 'group' | 'user',
                  })
                }
              />
              <Input
                label={localConfig.pii_access_mode === 'group' ? 'Group Name' : 'Username'}
                placeholder={localConfig.pii_access_mode === 'group' ? 'Enter group name' : 'Enter username'}
                value={localConfig.pii_access_value}
                onChange={(e) =>
                  setLocalConfig({ ...localConfig, pii_access_value: e.target.value })
                }
              />
            </div>
          </div>

          {/* PHI Access Control */}
          <div className="p-4 bg-warning-50 border border-warning-200 rounded-lg">
            <div className="flex items-center gap-2 mb-3">
              <Lock className="w-4 h-4 text-warning-600" />
              <h4 className="text-sm font-semibold text-gray-900">PHI Data Access</h4>
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <Select
                label="Access Control Type"
                options={[
                  { value: 'group', label: 'Allow by Group' },
                  { value: 'user', label: 'Allow by User' },
                ]}
                value={localConfig.phi_access_mode}
                onChange={(e) =>
                  setLocalConfig({
                    ...localConfig,
                    phi_access_mode: e.target.value as 'group' | 'user',
                  })
                }
              />
              <Input
                label={localConfig.phi_access_mode === 'group' ? 'Group Name' : 'Username'}
                placeholder={localConfig.phi_access_mode === 'group' ? 'Enter group name' : 'Enter username'}
                value={localConfig.phi_access_value}
                onChange={(e) =>
                  setLocalConfig({ ...localConfig, phi_access_value: e.target.value })
                }
              />
            </div>
          </div>

          <Button
            variant="primary"
            onClick={handleGenerateMaskingSQL}
            isLoading={isGenerating}
            icon={<Shield className="w-4 h-4" />}
          >
            Generate Masking SQL
          </Button>
        </div>
      </Card>

      {/* Generated Masking SQL */}
      {masking_sql && (
        <Card>
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900">
              Generated Masking SQL
            </h3>
            <Button
              variant="primary"
              size="sm"
              onClick={handleExecuteMasking}
              isLoading={isExecuting}
              disabled={mask_execution_status === 'RUNNING'}
              icon={<Play className="w-4 h-4" />}
            >
              Execute Masking SQL
            </Button>
          </div>

          <CodeViewer code={masking_sql} language="sql" />

          {/* Execution Status */}
          {mask_execution_status !== 'NOT_STARTED' && (
            <div className="mt-4">
              <ExecutionLog 
                status={mask_execution_status} 
                logs={mask_execution_log} 
              />
            </div>
          )}
        </Card>
      )}
    </>
  );
};
