import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo } from 'react';
const TYPE_ORDER = ['dimension', 'table', 'fact'];
const CARD_WIDTH = 280;
const CARD_MIN_HEIGHT = 120;
const CARD_HEADER_HEIGHT = 34;
const CARD_ROW_HEIGHT = 20;
const LANE_GAP = 180;
const ROW_GAP = 36;
function classifyTableType(name) {
    const normalized = (name || '').toLowerCase();
    if (normalized.startsWith('dim_'))
        return 'dimension';
    if (normalized.startsWith('fact_'))
        return 'fact';
    return 'table';
}
function parseSqlFallback(sqlText) {
    const cleanSql = sqlText.replace(/`/g, '').replace(/IF NOT EXISTS/gi, '');
    const createRegex = /CREATE\s+TABLE\s+([\w.]+)\s*\(([\s\S]*?)\)\s*;?/gi;
    const nodes = [];
    let match;
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
    if (!nodes.length)
        return null;
    return { nodes, edges: [] };
}
function normalizeDiagram(diagram) {
    if (!diagram?.tables?.length)
        return null;
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
function sanitizeGraph(graph) {
    return {
        nodes: (graph.nodes || []).map((n) => ({
            ...n,
            table_type: TYPE_ORDER.includes(n.table_type)
                ? n.table_type
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
function buildLayout(graph) {
    const lanes = {
        dimension: [],
        table: [],
        fact: [],
    };
    graph.nodes.forEach((node) => {
        const type = node.table_type || 'table';
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
    const positioned = [];
    let maxHeight = 0;
    TYPE_ORDER.forEach((type, laneIdx) => {
        lanes[type].forEach((node, rowIdx) => {
            node.x = laneIdx * (CARD_WIDTH + LANE_GAP) + 24;
            node.y = rowIdx * (CARD_MIN_HEIGHT + ROW_GAP) + 24;
            positioned.push(node);
            maxHeight = Math.max(maxHeight, node.y + node.height);
        });
    });
    const relations = graph.edges
        .map((edge, idx) => {
        const fromNode = positioned.find((n) => n.tableName === edge.from_table);
        const toNode = positioned.find((n) => n.tableName === edge.to_table);
        if (!fromNode || !toNode)
            return null;
        const fromPoint = fromNode.x <= toNode.x
            ? { x: fromNode.x + fromNode.width, y: fromNode.y + fromNode.height / 2 }
            : { x: fromNode.x, y: fromNode.y + fromNode.height / 2 };
        const toPoint = fromNode.x <= toNode.x
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
            fromCardinality: (edge.from_cardinality || 'N'),
            toCardinality: (edge.to_cardinality || '1'),
            label: `${edge.from_column} to ${edge.to_column}`,
        };
    })
        .filter(Boolean);
    const width = TYPE_ORDER.length * (CARD_WIDTH + LANE_GAP) + 80;
    const height = Math.max(maxHeight + 60, 280);
    return { width, height, nodes: positioned, relations };
}
function relationshipDiamond(mid, size = 14) {
    return `${mid.x} ${mid.y - size}, ${mid.x + size} ${mid.y}, ${mid.x} ${mid.y + size}, ${mid.x - size} ${mid.y}`;
}
export const ERDiagram = ({ sql, diagram, graph }) => {
    const sourceGraph = useMemo(() => {
        const pick = graph?.nodes?.length ? graph : normalizeDiagram(diagram) || parseSqlFallback(sql);
        return pick ? sanitizeGraph(pick) : null;
    }, [graph, diagram, sql]);
    if (!sourceGraph || !sourceGraph.nodes.length) {
        return (_jsx("div", { className: "bg-white border border-gray-200 rounded-2xl p-6 shadow-sm mb-6", children: _jsx("p", { className: "text-sm text-gray-500", children: "Generate model SQL to render ER diagram." }) }));
    }
    const layout = buildLayout(sourceGraph);
    return (_jsxs("div", { className: "bg-gradient-to-br from-white via-slate-50 to-slate-100 border border-gray-200 rounded-2xl p-6 shadow-lg mb-6", children: [_jsxs("div", { className: "mb-4", children: [_jsx("h4", { className: "text-lg font-semibold text-gray-900", children: "Entity Relationship Diagram" }), _jsx("p", { className: "text-xs text-gray-500 mt-1", children: "Entities, attributes, and relationship cardinality (`1` / `N`) inferred from generated schema." })] }), _jsx("div", { className: "overflow-auto rounded-xl border border-slate-200 bg-white/80", children: _jsxs("svg", { width: layout.width, height: layout.height, viewBox: `0 0 ${layout.width} ${layout.height}`, children: [_jsx("defs", { children: _jsx("marker", { id: "er-arrow", viewBox: "0 0 10 10", refX: "8", refY: "5", markerWidth: "7", markerHeight: "7", orient: "auto", children: _jsx("path", { d: "M0,0 L10,5 L0,10 z", fill: "#334155" }) }) }), layout.relations.map((rel) => (_jsxs("g", { children: [_jsx("line", { x1: rel.fromPoint.x, y1: rel.fromPoint.y, x2: rel.midPoint.x, y2: rel.midPoint.y, stroke: "#475569", strokeWidth: 1.8 }), _jsx("line", { x1: rel.midPoint.x, y1: rel.midPoint.y, x2: rel.toPoint.x, y2: rel.toPoint.y, stroke: "#475569", strokeWidth: 1.8, markerEnd: "url(#er-arrow)" }), _jsx("polygon", { points: relationshipDiamond(rel.midPoint), fill: "#E2E8F0", stroke: "#64748B", strokeWidth: 1.4 }), _jsx("text", { x: rel.midPoint.x, y: rel.midPoint.y + 4, textAnchor: "middle", fontSize: "9", fill: "#334155", children: "R" }), _jsx("text", { x: rel.midPoint.x, y: rel.midPoint.y - 20, textAnchor: "middle", fontSize: "9", fill: "#334155", children: rel.label }), _jsx("text", { x: rel.fromPoint.x + (rel.fromPoint.x < rel.midPoint.x ? 10 : -10), y: rel.fromPoint.y - 8, textAnchor: rel.fromPoint.x < rel.midPoint.x ? 'start' : 'end', fontSize: "11", fontWeight: "700", fill: "#1D4ED8", children: rel.fromCardinality }), _jsx("text", { x: rel.toPoint.x + (rel.midPoint.x < rel.toPoint.x ? -10 : 10), y: rel.toPoint.y - 8, textAnchor: rel.midPoint.x < rel.toPoint.x ? 'end' : 'start', fontSize: "11", fontWeight: "700", fill: "#166534", children: rel.toCardinality })] }, rel.id))), layout.nodes.map((node) => (_jsxs("g", { children: [_jsx("rect", { x: node.x, y: node.y, width: node.width, height: node.height, rx: 10, ry: 10, fill: node.type === 'dimension' ? '#EFF6FF' : node.type === 'fact' ? '#ECFDF5' : '#F8FAFC', stroke: node.type === 'dimension' ? '#60A5FA' : node.type === 'fact' ? '#34D399' : '#94A3B8', strokeWidth: 1.3 }), _jsx("line", { x1: node.x, y1: node.y + CARD_HEADER_HEIGHT, x2: node.x + node.width, y2: node.y + CARD_HEADER_HEIGHT, stroke: "#CBD5E1" }), _jsx("text", { x: node.x + 12, y: node.y + 22, fontSize: "13", fontWeight: "700", fill: "#0F172A", children: node.tableName }), _jsx("text", { x: node.x + node.width - 12, y: node.y + 22, textAnchor: "end", fontSize: "9", fontWeight: "700", fill: "#334155", children: node.type.toUpperCase() }), node.columns.map((column, index) => (_jsxs("text", { x: node.x + 12, y: node.y + CARD_HEADER_HEIGHT + 16 + index * CARD_ROW_HEIGHT, fontSize: "11", fill: "#334155", children: [column.is_primary_key ? 'PK ' : column.is_foreign_key ? 'FK ' : '', column.name, column.data_type ? ` : ${column.data_type}` : ''] }, `${node.id}-${column.name}`)))] }, node.id)))] }) })] }));
};
