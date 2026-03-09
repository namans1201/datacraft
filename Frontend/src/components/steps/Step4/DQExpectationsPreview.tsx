import React from 'react';
import { DataTable } from '@/components/common/DataTable';
import { Badge } from '@/components/common/Badge';
import { DQExpectation } from '../../../types/ui';

interface DQExpectationsPreviewProps {
  expectations: DQExpectation[];
}

export const DQExpectationsPreview: React.FC<DQExpectationsPreviewProps> = ({ 
  expectations 
}) => {
  const columns = [
    {
      key: 'table',
      header: 'Table',
      width: '20%',
    },
    {
      key: 'rule_name',
      header: 'Rule Name',
      width: '25%',
    },
    {
      key: 'condition',
      header: 'Condition',
      width: '35%',
      render: (value: unknown) => (
        <code className="text-xs bg-gray-100 px-2 py-1 rounded">
          {String(value)}
        </code>
      ),
    },
    {
      key: 'enforcement',
      header: 'Enforcement',
      width: '20%',
      render: (value: unknown) => {
        const enforcement = value as 'FAIL' | 'DROP' | 'LOG';
        return (
        <Badge variant={enforcement} size="sm">
          {enforcement}
        </Badge>
      )},
    },
  ];

  return (
    <div>
      <p className="text-sm text-gray-600 mb-4">
        Data Quality rules that will be enforced on each layer. 
        <span className="font-medium"> FAIL</span> stops the pipeline, 
        <span className="font-medium"> DROP</span> removes invalid rows, 
        <span className="font-medium"> LOG</span> records violations.
      </p>
      <DataTable 
        columns={columns} 
        data={expectations} 
        emptyMessage="No DQ expectations available"
      />
    </div>
  );
};
