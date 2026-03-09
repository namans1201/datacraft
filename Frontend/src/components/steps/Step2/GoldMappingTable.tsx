import React from 'react';
import { DataTable } from '@/components/common/DataTable';
import { Badge } from '@/components/common/Badge';
import { GoldMappingRow } from '../../../types/agent-state';

interface GoldMappingTableProps {
  mappings: GoldMappingRow[];
}

export const GoldMappingTable: React.FC<GoldMappingTableProps> = ({ mappings }) => {
  const columns = [
    {
      key: 'silver_table',
      header: 'Silver Table',
      width: '20%',
    },
    {
      key: 'silver_column',
      header: 'Silver Column',
      width: '20%',
    },
    {
      key: 'gold_table',
      header: 'Gold Table',
      width: '20%',
    },
    {
      key: 'gold_column',
      header: 'Gold Column',
      width: '20%',
    },
    
    {
      key: 'transformation',
      header: 'Transformation',
      width: '10%',
      render: (value: unknown) => (
        <span className="text-xs text-gray-600">{String(value || 'direct')}</span>
      ),
    },
  ];

  return <DataTable columns={columns} data={mappings} emptyMessage="No gold mappings generated yet" />;
};
