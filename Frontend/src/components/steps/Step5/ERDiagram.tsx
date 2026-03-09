import React, { useMemo } from 'react';
import { ERDiagram as ERDiagramType, ERDiagramGraph } from '@/types/agent-state';

interface ERDiagramProps {
  sql: string;
  diagram?: ERDiagramType;
  graph?: ERDiagramGraph;
}

const TYPE_ORDER: Array<'dimension' | 'table' | 'fact'> = ['dimension', 'table', 'fact'];
const CARD_WIDTH = 240;
const CARD_PADDING = 16;
const COLUMN_GAP = 140;
const ROW_GAP = 24;
const NODE_HEIGHT = (base: number) => Math.max(base, 120);

interface LayoutNode {
  id: string;
  tableName: string;
  type: 'dimension' | 'fact' | 'table';
  columns: { name: string; data_type?: string; is_primary_key?: boolean; is_foreign_key?: boolean }[];
  x: number;
  y: number;
  width: number;
  height: number;
}

interface LayoutEdge {
  from: LayoutNode;
  to: LayoutNode;
  fromPoint: { x: number; y: number };
  toPoint: { x: number; y: number };
}

function parseSqlDiagram(sqlText: string): ERDiagramGraph | null {
  const cleanSql = sqlText.replace(/`/g, '').replace(/IF NOT EXISTS/gi, '');
  const tableRegex = /CREATE\\s+TABLE\\s+([\\w.]+)\\s*\\(([^)]+)\\)/gi;
  const tables: Record<string, ERDiagramType['tables'][number]> = {};
  let match: RegExpExecArray | null;

  while ((match = tableRegex.exec(cleanSql)) !== null) {
    const name = match[1];
    const colsSection = match[2];
    const columns = colsSection
      .split(',')
      .map((line) => line.trim())
      .filter((line) => line && !/CONSTRAINT|PRIMARY KEY|FOREIGN KEY/i.test(line))
      .map((line) => {
        const [colName, type = ''] = line.split(/\\s+/);
        return {
          name: colName,
          data_type: type,
          is_primary_key: /PRIMARY KEY/i.test(line),
          is_foreign_key: /REFERENCES/i.test(line),
        };
      });
    tables[name] = { name, columns };
  }

  if (!Object.keys(tables).length) return null;
  return { nodes: Object.values(tables).map((table) => ({ table_name: table.name, columns: table.columns, table_type: classifyTableType(table.name) })), edges: [] };
}

function classifyTableType(name?: string): 'dimension' | 'fact' | 'table' {
  const normalized = (name || '').toLowerCase();
  if (normalized.startsWith('dim_')) return 'dimension';
  if (normalized.startsWith('fact_')) return 'fact';
  return 'table';
}

function normalizeDiagram(diagram?: ERDiagramType): ERDiagramGraph | null {
  if (!diagram || !diagram.tables.length) return null;
  return {
    nodes: diagram.tables.map((table) => ({
      table_name: table.name,
      columns: table.columns,
      table_type: classifyTableType(table.name),
    })),
    edges: diagram.relationships.map((rel) => ({
      from_table: rel.from_table,
      from_column: rel.from_column,
      to_table: rel.to_table,
      to_column: rel.to_column,
    })),
  };
}

function buildLayout(graph: ERDiagramGraph): { nodes: LayoutNode[]; edges: LayoutEdge[]; width: number; height: number } {
  const columns: Record<string, LayoutNode[]> = {};
  TYPE_ORDER.forEach((type) => {
    columns[type] = [];
  });

  graph.nodes.forEach((node) => {
    const type = TYPE_ORDER.includes(node.table_type) ? node.table_type : 'table';
    const columnsList = columns[type];
    const height = CARD_PADDING * 2 + 24 + node.columns.length * 20;
    columnsList.push({
      id: `${type}:${node.table_name}`,
      tableName: node.table_name,
      type,
      columns: node.columns,
      x: 0,
      y: 0,
      width: CARD_WIDTH,
      height,
    });
  });

  let maxHeight = 0;
  const nodePositions: LayoutNode[] = [];
  TYPE_ORDER.forEach((type, idx) => {
    const nodes = columns[type];
    nodes.forEach((node, row) => {
      node.x = idx * (CARD_WIDTH + COLUMN_GAP);
      node.y = row * (NODE_HEIGHT(node.height) + ROW_GAP);
      maxHeight = Math.max(maxHeight, node.y + node.height);
      nodePositions.push(node);
    });
  });

  const edges: LayoutEdge[] = graph.edges
    .map((edge) => {
      const fromNode = nodePositions.find((n) => n.tableName === edge.from_table);
      const toNode = nodePositions.find((n) => n.tableName === edge.to_table);
      if (!fromNode || !toNode) return null;
      const fromPoint = { x: fromNode.x + fromNode.width, y: fromNode.y + fromNode.height / 2 };
      const toPoint = { x: toNode.x, y: toNode.y + toNode.height / 2 };
      return { from: fromNode, to: toNode, fromPoint, toPoint };
    })
    .filter(Boolean) as LayoutEdge[];

  const width = Math.max(TYPE_ORDER.length * (CARD_WIDTH + COLUMN_GAP), 0);
  return { nodes: nodePositions, edges, width, height: Math.max(maxHeight, 0) };
}

export const ERDiagram: React.FC<ERDiagramProps> = ({ sql, diagram, graph }) => {
  const parsedGraph = useMemo(() => {
    return graph?.nodes?.length ? graph : normalizeDiagram(diagram) || parseSqlDiagram(sql);
  }, [graph, diagram, sql]);

  if (!parsedGraph || !parsedGraph.nodes.length) {
    return (
      <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm mb-6 min-h-[200px]">
        <div className="text-sm text-gray-500">ER diagram will appear here once the model is generated.</div>
      </div>
    );
  }

  const layout = buildLayout(parsedGraph);

  return (
    <div className="bg-gradient-to-br from-white to-slate-50 border border-gray-200 rounded-2xl p-6 shadow-lg mb-6">
      <div className="flex items-center justify-between mb-4">
        <h4 className="text-lg font-semibold text-gray-900">Dimensional Model ER Diagram</h4>
      </div>
      <div className="overflow-auto">
        <svg
          width={layout.width + 80}
          height={layout.height + 40}
          viewBox={`0 0 ${layout.width + 80} ${layout.height + 40}`}
        >
          <defs>
            <marker
              id="arrow"
              viewBox="0 0 6 6"
              refX="5"
              refY="3"
              markerWidth="6"
              markerHeight="6"
              orient="auto"
            >
              <path d="M0,0 L6,3 L0,6 Z" fill="#2563EB" />
            </marker>
          </defs>
          {layout.edges.map((edge, idx) => (
            <path
              key={`edge-${idx}`}
              d={`M${edge.fromPoint.x} ${edge.fromPoint.y} C${edge.fromPoint.x + 40} ${
                edge.fromPoint.y
              }, ${edge.toPoint.x - 40} ${edge.toPoint.y}, ${edge.toPoint.x} ${edge.toPoint.y}`}
              stroke="#2563EB"
              strokeWidth={2}
              fill="none"
              markerEnd="url(#arrow)"
              opacity={0.85}
            />
          ))}
          {layout.nodes.map((node) => (
            <g key={node.id}>
              <foreignObject
                x={node.x}
                y={node.y}
                width={node.width}
                height={node.height}
              >
                <div className="h-full w-full bg-white ring-1 ring-slate-200 rounded-2xl shadow-sm p-4 flex flex-col">
                  <div className="flex items-center justify-between mb-3">
                    <span className="text-sm font-semibold text-slate-900">{node.tableName}</span>
                    <span
                      className={`px-2 py-0.5 text-[10px] font-semibold uppercase rounded-full ${
                        node.type === 'dimension'
                          ? 'bg-blue-100 text-blue-700'
                          : node.type === 'fact'
                            ? 'bg-emerald-100 text-emerald-700'
                            : 'bg-slate-100 text-slate-600'
                      }`}
                    >
                      {node.type}
                    </span>
                  </div>
                  <div className="space-y-1 text-[12px] text-slate-600 flex-1">
                    {node.columns.map((column) => (
                      <div key={`${node.id}-${column.name}`} className="flex items-center gap-2">
                        <span className="font-mono">{column.name}</span>
                        {column.data_type && <span className="text-xs text-slate-400">{column.data_type}</span>}
                        {column.is_primary_key && (
                          <span className="text-xs uppercase text-blue-600 font-semibold">PK</span>
                        )}
                        {column.is_foreign_key && (
                          <span className="text-xs uppercase text-emerald-600 font-semibold">FK</span>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </foreignObject>
              <rect
                x={node.x}
                y={node.y}
                width={node.width}
                height={node.height}
                rx={24}
                ry={24}
                fill="transparent"
              />
            </g>
          ))}
        </svg>
      </div>
    </div>
  );
};
