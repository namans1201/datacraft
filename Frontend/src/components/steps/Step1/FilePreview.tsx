import React, { useState, useMemo } from 'react';
import { Shield, ShieldAlert, Eye, EyeOff } from 'lucide-react';
import { DataTable } from '@/components/common/DataTable';
// FIX 1: Added missing Button import
import { Button } from '@/components/common/Button'; 

interface FilePreviewProps {
  fileName: string;
  data: {
    columns: string[];
    data: any[];
    sensitivity: Record<string, string>;
  };
}

const SENSITIVITY_OPTIONS = ["NON_SENSITIVE", "PII", "PCI", "PHI"];

export const FilePreview: React.FC<FilePreviewProps> = ({ fileName, data }) => {
  const [localSensitivity, setLocalSensitivity] = useState(data.sensitivity || {});
  const [maskEnabled, setMaskEnabled] = useState(false);

  const handleSensitivityChange = (col: string, value: string) => {
    setLocalSensitivity(prev => ({ ...prev, [col]: value }));
  };

  const maskValue = (val: any, col: string) => {
    if (val === null || val === undefined || val === '') return "-";
    
    if (!maskEnabled || localSensitivity[col] === "NON_SENSITIVE") return val;
    
    const str = String(val);
    if (str.length <= 2) return "**";
    return `${str[0]}***${str[str.length - 1]}`;
  };







  const tableColumns = useMemo(() => {
    return data.columns.map((col, index) => ({
      key: col,
      header: col,
      render: (row: any) => {

        let value;

        if (typeof row !== 'object' || row === null) {
          value = row; 
        } else {
          const actualKey = Object.keys(row).find(
            key => key.toLowerCase() === col.toLowerCase()
          );
          value = actualKey ? row[actualKey] : row[index];
        }

        return (
          <span className="font-mono text-sm text-gray-700">
            {maskValue(value, col)}
          </span>
        );
      }
    }));
  }, [data.columns, maskEnabled, localSensitivity, maskValue]);

  return (
    <div className="space-y-6 mb-10">
      <div className="flex justify-between items-center">
        <h4 className="text-lg font-bold text-gray-900">{fileName}</h4>
        
        <Button 
          variant={maskEnabled ? "danger" : "secondary"}
          size="sm"
          onClick={() => setMaskEnabled(!maskEnabled)}
          className="flex items-center gap-2"
        >
          {maskEnabled ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
          {maskEnabled ? "Masking On" : "Masking Off"}
        </Button>
      </div>

      {/* --- TABLE 1: Sensitivity Classification (Control Row) --- */}
      <div className="border border-gray-200 rounded-lg overflow-hidden shadow-sm">
        <div className="bg-gray-50 px-4 py-2 border-b border-gray-200 flex items-center gap-2">
          <Shield className="w-4 h-4 text-gray-500" />
          <span className="text-xs font-bold text-gray-500 uppercase">Column Classification (AI Suggested)</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <tbody className="bg-white">
              <tr className="divide-x divide-gray-100">
                {data.columns.map(col => (
                  <td key={col} className="px-4 py-3 min-w-[150px]">
                    <div className="flex flex-col gap-1">
                      <span className="text-[10px] font-bold text-gray-400 uppercase truncate">{col}</span>
                      <select
                        value={localSensitivity[col] || "NON_SENSITIVE"}
                        onChange={(e) => handleSensitivityChange(col, e.target.value)}
                        className={`text-xs font-semibold rounded-md border-gray-200 p-1.5 focus:ring-2 focus:ring-primary-500 w-full cursor-pointer transition-colors ${
                          localSensitivity[col] !== 'NON_SENSITIVE' 
                            ? 'bg-orange-50 text-orange-700 border-orange-200' 
                            : 'bg-emerald-50 text-emerald-700 border-emerald-200'
                        }`}
                      >
                        {SENSITIVITY_OPTIONS.map(opt => <option key={opt} value={opt}>{opt}</option>)}
                      </select>
                    </div>
                  </td>
                ))}
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* --- TABLE 2: Data Preview --- */}
      <div className="border border-gray-200 rounded-lg overflow-hidden shadow-sm">
        <div className="bg-gray-50 px-4 py-2 border-b border-gray-200">
          <span className="text-xs font-bold text-gray-500 uppercase">Data Preview (Sample)</span>
        </div>
        <DataTable 
          columns={tableColumns} 
          data={data.data} 
          className="bg-white" 
        />
      </div>
    </div>
  );
};
