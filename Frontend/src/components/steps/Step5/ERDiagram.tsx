import React, { useMemo } from 'react';
import { ERDiagram as ERDiagramType, ERDiagramGraph } from '@/types/agent-state';

interface ERDiagramProps {
  sql: string;
  diagram?: ERDiagramType;
  graph?: ERDiagramGraph;
}

type NodeType = 'dimension' | 'table' | 'fact';

interface LayoutNode {
  id: string;
  tableName: string;
  type: NodeType;
  columns: Array<{
    name: string;
    data_type?: string;
    is_primary_key?: boolean;
    is_foreign_key?: boolean;
  }>;
  x: number;
  y: number;
  width: number;
  height: number;
}

interface LayoutRelation {
  id: string;
  fromNode: LayoutNode;
  toNode: LayoutNode;
  fromPoint: { x: number; y: number };
  toPoint: { x: number; y: number };
  midPoint: { x: number; y: number };
  fromCardinality: '1' | 'N';
  toCardinality: '1' | 'N';
  label: string;
}

const TYPE_ORDER: NodeType[] = ['dimension', 'table', 'fact'];
const CARD_WIDTH = 280;
const CARD_MIN_HEIGHT = 120;
const CARD_HEADER_HEIGHT = 34;
const CARD_ROW_HEIGHT = 20;
const LANE_GAP = 180;
const ROW_GAP = 36;

function classifyTableType(name?: string): NodeType {
  const normalized = (name || '').toLowerCase();
  if (normalized.startsWith('dim_')) return 'dimension';
  if (normalized.startsWith('fact_')) return 'fact';
  return 'table';
}

function parseSqlFallback(sqlText: string): ERDiagramGraph | null {
  const cleanSql = sqlText.replace(/`/g, '').replace(/IF NOT EXISTS/gi, '');
  const createRegex = /CREATE\s+TABLE\s+([\w.]+)\s*\(([\s\S]*?)\)\s*;?/gi;
  const nodes: ERDiagramGraph['nodes'] = [];

  let match: RegExpExecArray | null;
  while ((match = createRegex.exec(cleanSql)) !== null) {
    const tableName = match[1];
    const columns = match[2]
      .split(',')
      .map((part) => part.trim())
      .filter((line) => line && !/^(CONSTRAINT|PRIMARY|FOREIGN|UNIQUE)\b/i.test(line))
      .map((line) => {
        const tokens = line.split(/\s+/);
        return {
          name: tokens[0] || 'column',
          data_type: tokens[1],
          is_primary_key: /PRIMARY\s+KEY/i.test(line),
          is_foreign_key: /REFERENCES/i.test(line),
        };
      });

    nodes.push({
      table_name: tableName,
      table_type: classifyTableType(tableName),
      columns,
    });
  }

  if (!nodes.length) return null;
  return { nodes, edges: [] };
}

function normalizeDiagram(diagram?: ERDiagramType): ERDiagramGraph | null {
  if (!diagram?.tables?.length) return null;
  return {
    nodes: diagram.tables.map((table) => ({
      table_name: table.name,
      table_type: classifyTableType(table.name),
      columns: table.columns,
    })),
    edges: (diagram.relationships || []).map((rel) => ({
      from_table: rel.from_table,
      from_column: rel.from_column,
      to_table: rel.to_table,
      to_column: rel.to_column,
      from_cardinality: rel.from_cardinality || 'N',
      to_cardinality: rel.to_cardinality || '1',
    })),
  };
}

function sanitizeGraph(graph: ERDiagramGraph): ERDiagramGraph {
  return {
    nodes: (graph.nodes || []).map((n) => ({
      ...n,
      table_type: TYPE_ORDER.includes(n.table_type as NodeType)
        ? (n.table_type as NodeType)
        : classifyTableType(n.table_name),
      columns: n.columns || [],
    })),
    edges: (graph.edges || []).map((e) => ({
      ...e,
      from_cardinality: e.from_cardinality || 'N',
      to_cardinality: e.to_cardinality || '1',
    })),
  };
}

function buildLayout(graph: ERDiagramGraph): {
  width: number;
  height: number;
  nodes: LayoutNode[];
  relations: LayoutRelation[];
} {
  const lanes: Record<NodeType, LayoutNode[]> = {
    dimension: [],
    table: [],
    fact: [],
  };

  graph.nodes.forEach((node) => {
    const type = (node.table_type as NodeType) || 'table';
    const height = Math.max(CARD_MIN_HEIGHT, CARD_HEADER_HEIGHT + 16 + node.columns.length * CARD_ROW_HEIGHT);
    lanes[type].push({
      id: `${type}:${node.table_name}`,
      tableName: node.table_name,
      type,
      columns: node.columns || [],
      x: 0,
      y: 0,
      width: CARD_WIDTH,
      height,
    });
  });

  const positioned: LayoutNode[] = [];
  let maxHeight = 0;

  TYPE_ORDER.forEach((type, laneIdx) => {
    lanes[type].forEach((node, rowIdx) => {
      node.x = laneIdx * (CARD_WIDTH + LANE_GAP) + 24;
      node.y = rowIdx * (CARD_MIN_HEIGHT + ROW_GAP) + 24;
      positioned.push(node);
      maxHeight = Math.max(maxHeight, node.y + node.height);
    });
  });

  const relations: LayoutRelation[] = graph.edges
    .map((edge, idx) => {
      const fromNode = positioned.find((n) => n.tableName === edge.from_table);
      const toNode = positioned.find((n) => n.tableName === edge.to_table);
      if (!fromNode || !toNode) return null;

      const fromPoint =
        fromNode.x <= toNode.x
          ? { x: fromNode.x + fromNode.width, y: fromNode.y + fromNode.height / 2 }
          : { x: fromNode.x, y: fromNode.y + fromNode.height / 2 };
      const toPoint =
        fromNode.x <= toNode.x
          ? { x: toNode.x, y: toNode.y + toNode.height / 2 }
          : { x: toNode.x + toNode.width, y: toNode.y + toNode.height / 2 };

      const midPoint = {
        x: (fromPoint.x + toPoint.x) / 2,
        y: (fromPoint.y + toPoint.y) / 2,
      };

      return {
        id: `rel-${idx}`,
        fromNode,
        toNode,
        fromPoint,
        toPoint,
        midPoint,
        fromCardinality: (edge.from_cardinality || 'N') as '1' | 'N',
        toCardinality: (edge.to_cardinality || '1') as '1' | 'N',
        label: `${edge.from_column} to ${edge.to_column}`,
      };
    })
    .filter(Boolean) as LayoutRelation[];

  const width = TYPE_ORDER.length * (CARD_WIDTH + LANE_GAP) + 80;
  const height = Math.max(maxHeight + 60, 280);
  return { width, height, nodes: positioned, relations };
}

function relationshipDiamond(mid: { x: number; y: number }, size = 14): string {
  return `${mid.x} ${mid.y - size}, ${mid.x + size} ${mid.y}, ${mid.x} ${mid.y + size}, ${mid.x - size} ${mid.y}`;
}

export const ERDiagram: React.FC<ERDiagramProps> = ({ sql, diagram, graph }) => {
  const sourceGraph = useMemo(() => {
    const pick = graph?.nodes?.length ? graph : normalizeDiagram(diagram) || parseSqlFallback(sql);
    return pick ? sanitizeGraph(pick) : null;
  }, [graph, diagram, sql]);

  if (!sourceGraph || !sourceGraph.nodes.length) {
    return (
      <div className="bg-white border border-gray-200 rounded-2xl p-6 shadow-sm mb-6">
        <p className="text-sm text-gray-500">Generate model SQL to render ER diagram.</p>
      </div>
    );
  }

  const layout = buildLayout(sourceGraph);

  return (
    <div className="bg-gradient-to-br from-white via-slate-50 to-slate-100 border border-gray-200 rounded-2xl p-6 shadow-lg mb-6">
      <div className="mb-4">
        <h4 className="text-lg font-semibold text-gray-900">Entity Relationship Diagram</h4>
        <p className="text-xs text-gray-500 mt-1">
          Entities, attributes, and relationship cardinality (`1` / `N`) inferred from generated schema.
        </p>
      </div>

      <div className="overflow-auto rounded-xl border border-slate-200 bg-white/80">
        <svg width={layout.width} height={layout.height} viewBox={`0 0 ${layout.width} ${layout.height}`}>
          <defs>
            <marker id="er-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0,0 L10,5 L0,10 z" fill="#334155" />
            </marker>
          </defs>

          {layout.relations.map((rel) => (
            <g key={rel.id}>
              <line
                x1={rel.fromPoint.x}
                y1={rel.fromPoint.y}
                x2={rel.midPoint.x}
                y2={rel.midPoint.y}
                stroke="#475569"
                strokeWidth={1.8}
              />
              <line
                x1={rel.midPoint.x}
                y1={rel.midPoint.y}
                x2={rel.toPoint.x}
                y2={rel.toPoint.y}
                stroke="#475569"
                strokeWidth={1.8}
                markerEnd="url(#er-arrow)"
              />
              <polygon points={relationshipDiamond(rel.midPoint)} fill="#E2E8F0" stroke="#64748B" strokeWidth={1.4} />
              <text x={rel.midPoint.x} y={rel.midPoint.y + 4} textAnchor="middle" fontSize="9" fill="#334155">
                R
              </text>
              <text x={rel.midPoint.x} y={rel.midPoint.y - 20} textAnchor="middle" fontSize="9" fill="#334155">
                {rel.label}
              </text>
              <text x={rel.fromPoint.x + (rel.fromPoint.x < rel.midPoint.x ? 10 : -10)} y={rel.fromPoint.y - 8} textAnchor={rel.fromPoint.x < rel.midPoint.x ? 'start' : 'end'} fontSize="11" fontWeight="700" fill="#1D4ED8">
                {rel.fromCardinality}
              </text>
              <text x={rel.toPoint.x + (rel.midPoint.x < rel.toPoint.x ? -10 : 10)} y={rel.toPoint.y - 8} textAnchor={rel.midPoint.x < rel.toPoint.x ? 'end' : 'start'} fontSize="11" fontWeight="700" fill="#166534">
                {rel.toCardinality}
              </text>
            </g>
          ))}

          {layout.nodes.map((node) => (
            <g key={node.id}>
              <rect
                x={node.x}
                y={node.y}
                width={node.width}
                height={node.height}
                rx={10}
                ry={10}
                fill={node.type === 'dimension' ? '#EFF6FF' : node.type === 'fact' ? '#ECFDF5' : '#F8FAFC'}
                stroke={node.type === 'dimension' ? '#60A5FA' : node.type === 'fact' ? '#34D399' : '#94A3B8'}
                strokeWidth={1.3}
              />
              <line x1={node.x} y1={node.y + CARD_HEADER_HEIGHT} x2={node.x + node.width} y2={node.y + CARD_HEADER_HEIGHT} stroke="#CBD5E1" />
              <text x={node.x + 12} y={node.y + 22} fontSize="13" fontWeight="700" fill="#0F172A">
                {node.tableName}
              </text>
              <text x={node.x + node.width - 12} y={node.y + 22} textAnchor="end" fontSize="9" fontWeight="700" fill="#334155">
                {node.type.toUpperCase()}
              </text>
              {node.columns.map((column, index) => (
                <text key={`${node.id}-${column.name}`} x={node.x + 12} y={node.y + CARD_HEADER_HEIGHT + 16 + index * CARD_ROW_HEIGHT} fontSize="11" fill="#334155">
                  {column.is_primary_key ? 'PK ' : column.is_foreign_key ? 'FK ' : ''}
                  {column.name}
                  {column.data_type ? ` : ${column.data_type}` : ''}
                </text>
              ))}
            </g>
          ))}
        </svg>
      </div>
    </div>
  );
};
