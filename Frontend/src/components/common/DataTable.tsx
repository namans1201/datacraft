import React from 'react';
import clsx from 'clsx';

interface Column<T> {
  key: string;
  header: string;
  render?: (value: unknown, row: T) => React.ReactNode;
  width?: string;
}

interface DataTableProps<T extends object> {
  columns: Column<T>[];
  data: T[];
  emptyMessage?: string;
  className?: string;
}


function renderValue(value: unknown): React.ReactNode {
  if (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return value.toString();
  }

  if (value == null) return '-';

  return JSON.stringify(value);
}



export function DataTable<T extends object>({
  columns,
  data,
  emptyMessage = 'No data available',
  className,
}: DataTableProps<T>) {
  return (
    <div className={clsx('overflow-x-auto', className)}>
      <table className="w-full border-collapse">
        <thead>
          <tr className="bg-gray-50 border-b-2 border-gray-200">
            {columns.map((col) => (
              <th
                key={String(col.key)}
                className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider"
                style={{ width: col.width }}
              >
                {col.header}
              </th>
            ))}
          </tr>
        </thead>

        <tbody className="bg-white divide-y divide-gray-200">
          {data.length === 0 ? (
            <tr>
              <td
                colSpan={columns.length}
                className="px-4 py-8 text-center text-sm text-gray-500"
              >
                {emptyMessage}
              </td>
            </tr>
          ) : (
            data.map((row, idx) => (
              <tr key={idx} className="hover:bg-gray-50">
                {columns.map((col) => {
                  const value = (row as Record<string, unknown>)[col.key];

                  return (
                    <td
                      key={String(col.key)}
                      className="px-4 py-3 text-sm text-gray-900"
                    >
                      {col.render
                        ? col.render(value, row)
                        : renderValue(value)}
                    </td>
                  );
                })}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
