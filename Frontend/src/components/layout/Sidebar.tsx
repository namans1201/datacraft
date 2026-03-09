import React, { useState } from 'react';
import { LayoutDashboard, Workflow, Eye, Table as TableIcon } from 'lucide-react';
import { clsx } from 'clsx';
import { Card } from '@/components/common/Card';
import { Input } from '@/components/common/Input';
import { Button } from '@/components/common/Button';
import { databricksApi } from '@/api/databricks';
import { ApiResponse } from '@/types/api';
import toast from 'react-hot-toast';

interface TableMetadata {
  Table: string;
  Columns: string;
}

export const Sidebar: React.FC = () => {
  const [activeNav, setActiveNav] = useState('pipeline');
  const [metadataForm, setMetadataForm] = useState({
    catalog: '',
    schema: '',
    token: '',
  });
  const [isLoading, setIsLoading] = useState(false);
  const [tables, setTables] = useState<TableMetadata[]>([]);

  const handleShowTables = async () => {
    if (!metadataForm.catalog || !metadataForm.schema || !metadataForm.token) {
      toast.error('Please fill all fields');
      return;
    }

    setIsLoading(true);
    setTables([]); // Clear previous results
    
    try {
      const response = await databricksApi.getTableMetadata(
        metadataForm.catalog,
        metadataForm.schema,
        metadataForm.token
      ) as ApiResponse<TableMetadata[]>;

      if (response.success) {
        toast.success('Tables loaded successfully');
        setTables(response.data || []);
      } else {
        toast.error(response.error || 'Failed to load tables');
      }
    } catch (error: any) {
      console.error("Frontend Error:", error);
      toast.error('Connection error. Check console for details.');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="w-72 bg-white border-r border-gray-200 h-full overflow-y-auto">
      <div className="p-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Navigation</h3>
        <div className="space-y-1">
          <button
            onClick={() => setActiveNav('pipeline')}
            className={clsx(
              'w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors',
              activeNav === 'pipeline' ? 'bg-primary-50 text-primary-700' : 'text-gray-700 hover:bg-gray-50'
            )}
          >
            <Workflow className="w-4 h-4" />
            Pipeline Builder
          </button>
          <button
            onClick={() => setActiveNav('dashboard')}
            className={clsx(
              'w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors',
              activeNav === 'dashboard' ? 'bg-primary-50 text-primary-700' : 'text-gray-700 hover:bg-gray-50'
            )}
          >
            <LayoutDashboard className="w-4 h-4" />
            Dashboard
          </button>
        </div>

        <div className="mt-8">
          <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
            <Eye className="w-4 h-4" />
            Metadata Viewer
          </h3>
          <Card padding="sm" className="bg-gray-50">
            <div className="space-y-3">
              <Input
                label="Catalog Name"
                placeholder="Enter catalog"
                value={metadataForm.catalog}
                onChange={(e) => setMetadataForm({ ...metadataForm, catalog: e.target.value })}
              />
              <Input
                label="Schema Name"
                placeholder="Enter schema"
                value={metadataForm.schema}
                onChange={(e) => setMetadataForm({ ...metadataForm, schema: e.target.value })}
              />
              <Input
                label="Access Token"
                type="password"
                placeholder="Enter token"
                value={metadataForm.token}
                onChange={(e) => setMetadataForm({ ...metadataForm, token: e.target.value })}
              />
              <Button
                variant="primary"
                size="sm"
                className="w-full"
                onClick={handleShowTables}
                isLoading={isLoading}
              >
                Show Tables
              </Button>
            </div>
          </Card>

          {/* TABLE DISPLAY AREA */}
          <div className="mt-6">
            {tables.length > 0 ? (
              <div className="space-y-4">
                <div className="flex items-center justify-between border-b border-gray-100 pb-2">
                  <h4 className="text-[10px] font-bold text-gray-400 uppercase tracking-widest">
                    Detected Tables ({tables.length})
                  </h4>
                </div>
                <div className="space-y-3 pb-10">
                  {tables.map((item, idx) => (
                    <Card key={idx} padding="sm" className="bg-white border-gray-100 hover:border-primary-200 transition-all shadow-sm group">
                      <div className="flex flex-col gap-1">
                        <div className="flex items-center gap-2 text-gray-900 group-hover:text-primary-600">
                          <TableIcon className="w-3.5 h-3.5" />
                          <span className="text-sm font-semibold truncate">{item.Table}</span>
                        </div>
                        <div className="mt-1.5 p-2 bg-gray-50 rounded border border-gray-100">
                          <span className="text-[9px] font-bold text-gray-400 uppercase">Columns</span>
                          <p className="text-[11px] text-gray-500 leading-relaxed mt-0.5 italic">
                            {item.Columns}
                          </p>
                        </div>
                      </div>
                    </Card>
                  ))}
                </div>
              </div>
            ) : (
              !isLoading && (
                <div className="text-center py-10">
                  <p className="text-xs text-gray-400 italic">No tables to display. Fill details and click fetch.</p>
                </div>
              )
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
