import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useState } from 'react';
import { Shield, Lock, Play } from 'lucide-react';
import { Card } from '@/components/common/Card';
import { Button } from '@/components/common/Button';
import { Select } from '@/components/common/Select';
import { Input } from '@/components/common/Input';
import { CodeViewer } from './CodeViewer';
import { ExecutionLog } from './ExecutionLog';
import { codegenApi } from '@/api/codegen';
import { useAgentStore } from '@/store/useAgentStore';
import toast from 'react-hot-toast';
export const MaskingPanel = () => {
    const { catalog, schema, masking_sql, mask_execution_status, mask_execution_log, pii_access_mode, pii_access_value, phi_access_mode, phi_access_value, setMaskingSQL, updateState, } = useAgentStore();
    const [localConfig, setLocalConfig] = useState({
        pii_access_mode: pii_access_mode ?? 'group',
        pii_access_value: pii_access_value || 'pii_access',
        phi_access_mode: phi_access_mode ?? 'group',
        phi_access_value: phi_access_value || 'phi_access',
    });
    const [isGenerating, setIsGenerating] = useState(false);
    const [isExecuting, setIsExecuting] = useState(false);
    const handleGenerateMaskingSQL = async () => {
        setIsGenerating(true);
        const request = {
            include_masking: true,
            catalog: catalog,
            schema_name: schema,
            pii_access_mode: localConfig.pii_access_mode,
            pii_access_value: localConfig.pii_access_value,
            phi_access_mode: localConfig.phi_access_mode,
            phi_access_value: localConfig.phi_access_value,
        };
        const result = (await codegenApi.generateMaskingSQL(request));
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
        }
        else {
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
        const result = (await codegenApi.executeMaskingSQL(catalog, schema));
        setIsExecuting(false);
        if (result.success) {
            toast.success('Masking SQL executed successfully');
            updateState({
                mask_execution_status: 'SUCCESS',
                mask_execution_log: (result.data?.execution_log || []).map((entry) => typeof entry === 'string'
                    ? {
                        timestamp: new Date().toISOString(),
                        level: 'info',
                        message: entry,
                    }
                    : {
                        timestamp: entry.timestamp || new Date().toISOString(),
                        level: entry.level || 'info',
                        message: entry.message || '',
                    }),
            });
        }
        else {
            toast.error(result.error || 'Failed to execute masking SQL');
            updateState({ mask_execution_status: 'FAILED' });
        }
    };
    return (_jsxs(_Fragment, { children: [_jsxs(Card, { children: [_jsxs("div", { className: "flex items-center gap-2 mb-4", children: [_jsx(Shield, { className: "w-5 h-5 text-warning-600" }), _jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Access Control & Data Masking" })] }), _jsx("p", { className: "text-sm text-gray-600 mb-6", children: "Configure access control policies and generate masking SQL functions for PII and PHI data protection." }), _jsxs("div", { className: "space-y-6", children: [_jsxs("div", { className: "p-4 bg-danger-50 border border-danger-200 rounded-lg", children: [_jsxs("div", { className: "flex items-center gap-2 mb-3", children: [_jsx(Lock, { className: "w-4 h-4 text-danger-600" }), _jsx("h4", { className: "text-sm font-semibold text-gray-900", children: "PII Data Access" })] }), _jsxs("div", { className: "grid grid-cols-2 gap-4", children: [_jsx(Select, { label: "Access Control Type", options: [
                                                    { value: 'group', label: 'Allow by Group' },
                                                    { value: 'user', label: 'Allow by User' },
                                                ], value: localConfig.pii_access_mode, onChange: (e) => setLocalConfig({
                                                    ...localConfig,
                                                    pii_access_mode: e.target.value,
                                                }) }), _jsx(Input, { label: localConfig.pii_access_mode === 'group' ? 'Group Name' : 'Username', placeholder: localConfig.pii_access_mode === 'group' ? 'Enter group name' : 'Enter username', value: localConfig.pii_access_value, onChange: (e) => setLocalConfig({ ...localConfig, pii_access_value: e.target.value }) })] })] }), _jsxs("div", { className: "p-4 bg-warning-50 border border-warning-200 rounded-lg", children: [_jsxs("div", { className: "flex items-center gap-2 mb-3", children: [_jsx(Lock, { className: "w-4 h-4 text-warning-600" }), _jsx("h4", { className: "text-sm font-semibold text-gray-900", children: "PHI Data Access" })] }), _jsxs("div", { className: "grid grid-cols-2 gap-4", children: [_jsx(Select, { label: "Access Control Type", options: [
                                                    { value: 'group', label: 'Allow by Group' },
                                                    { value: 'user', label: 'Allow by User' },
                                                ], value: localConfig.phi_access_mode, onChange: (e) => setLocalConfig({
                                                    ...localConfig,
                                                    phi_access_mode: e.target.value,
                                                }) }), _jsx(Input, { label: localConfig.phi_access_mode === 'group' ? 'Group Name' : 'Username', placeholder: localConfig.phi_access_mode === 'group' ? 'Enter group name' : 'Enter username', value: localConfig.phi_access_value, onChange: (e) => setLocalConfig({ ...localConfig, phi_access_value: e.target.value }) })] })] }), _jsx(Button, { variant: "primary", onClick: handleGenerateMaskingSQL, isLoading: isGenerating, icon: _jsx(Shield, { className: "w-4 h-4" }), children: "Generate Masking SQL" })] })] }), masking_sql && (_jsxs(Card, { children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h3", { className: "text-lg font-semibold text-gray-900", children: "Generated Masking SQL" }), _jsx(Button, { variant: "primary", size: "sm", onClick: handleExecuteMasking, isLoading: isExecuting, disabled: mask_execution_status === 'RUNNING', icon: _jsx(Play, { className: "w-4 h-4" }), children: "Execute Masking SQL" })] }), _jsx(CodeViewer, { code: masking_sql, language: "sql" }), mask_execution_status !== 'NOT_STARTED' && (_jsx("div", { className: "mt-4", children: _jsx(ExecutionLog, { status: mask_execution_status, logs: mask_execution_log }) }))] }))] }));
};
