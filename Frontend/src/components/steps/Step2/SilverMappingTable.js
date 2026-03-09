import { jsx as _jsx } from "react/jsx-runtime";
import { DataTable } from '@/components/common/DataTable';
export const SilverMappingTable = ({ mappings }) => {
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
    return _jsx(DataTable, { columns: columns, data: mappings, emptyMessage: "No mappings generated yet" });
};
