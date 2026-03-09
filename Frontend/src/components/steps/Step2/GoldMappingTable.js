import { jsx as _jsx } from "react/jsx-runtime";
import { DataTable } from '@/components/common/DataTable';
export const GoldMappingTable = ({ mappings }) => {
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
            render: (value) => (_jsx("span", { className: "text-xs text-gray-600", children: String(value || 'direct') })),
        },
    ];
    return _jsx(DataTable, { columns: columns, data: mappings, emptyMessage: "No gold mappings generated yet" });
};
