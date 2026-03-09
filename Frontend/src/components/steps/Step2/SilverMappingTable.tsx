import React from 'react';
import { DataTable } from '@/components/common/DataTable';
import { Badge } from '@/components/common/Badge';
import { MappingRow } from '../../../types/agent-state';

interface SilverMappingTableProps {
  mappings: MappingRow[];
}

export const SilverMappingTable: React.FC<SilverMappingTableProps> = ({ mappings }) => {
  const columns = [
    {
      key: 'bronze_table',
      header: 'Bronze Table',
      width: '20%',
    },
    {
      key: 'bronze_columns',
      header: 'Bronze Column',
      width: '25%',
    },
    {
      key: 'silver_table',
      header: 'Silver Table',
      width: '20%',
    },
    {
      key: 'silver_column',
      header: 'Silver Column',
      width: '25%',
    },
    // {
    //   key: 'data_classification',
    //   header: 'Classification',
    //   width: '10%',
    //   render: (value: string) => {
    //     if (!value || value === 'NON_SENSITIVE') return null;
    //     return (
    //       <Badge variant={value as any} size="sm">
    //         {value}
    //       </Badge>
    //     );
    //   },
    // },
  ];

  return <DataTable columns={columns} data={mappings} emptyMessage="No mappings generated yet" />;
};