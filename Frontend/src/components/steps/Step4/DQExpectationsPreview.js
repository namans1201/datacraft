import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { DataTable } from '@/components/common/DataTable';
import { Badge } from '@/components/common/Badge';
export const DQExpectationsPreview = ({ expectations }) => {
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
            render: (value) => (_jsx("code", { className: "text-xs bg-gray-100 px-2 py-1 rounded", children: String(value) })),
        },
        {
            key: 'enforcement',
            header: 'Enforcement',
            width: '20%',
            render: (value) => {
                const enforcement = value;
                return (_jsx(Badge, { variant: enforcement, size: "sm", children: enforcement }));
            },
        },
    ];
    return (_jsxs("div", { children: [_jsxs("p", { className: "text-sm text-gray-600 mb-4", children: ["Data Quality rules that will be enforced on each layer.", _jsx("span", { className: "font-medium", children: " FAIL" }), " stops the pipeline,", _jsx("span", { className: "font-medium", children: " DROP" }), " removes invalid rows,", _jsx("span", { className: "font-medium", children: " LOG" }), " records violations."] }), _jsx(DataTable, { columns: columns, data: expectations, emptyMessage: "No DQ expectations available" })] }));
};
