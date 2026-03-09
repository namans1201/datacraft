import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useMemo } from 'react';
const TYPE_ORDER = ['dimension', 'table', 'fact'];
const CARD_WIDTH = 240;
const CARD_PADDING = 16;
const COLUMN_GAP = 140;
const ROW_GAP = 24;
const NODE_HEIGHT = (base) => Math.max(base, 120);
function parseSqlDiagram(sqlText) {
    const cleanSql = sqlText.replace(/`/g, '').replace(/IF NOT EXISTS/gi, '');
    const tableRegex = /CREATE\\s+TABLE\\s+([\\w.]+)\\s*\\(([^)]+)\\)/gi;
    const tables = {};
    let match;
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
    if (!Object.keys(tables).length)
        return null;
    return { nodes: Object.values(tables).map((table) => ({ table_name: table.name, columns: table.columns, table_type: classifyTableType(table.name) })), edges: [] };
}
function classifyTableType(name) {
    const normalized = (name || '').toLowerCase();
    if (normalized.startsWith('dim_'))
        return 'dimension';
    if (normalized.startsWith('fact_'))
        return 'fact';
    return 'table';
}
function normalizeDiagram(diagram) {
    if (!diagram || !diagram.tables.length)
        return null;
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
function buildLayout(graph) {
    const columns = {};
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
    const nodePositions = [];
    TYPE_ORDER.forEach((type, idx) => {
        const nodes = columns[type];
        nodes.forEach((node, row) => {
            node.x = idx * (CARD_WIDTH + COLUMN_GAP);
            node.y = row * (NODE_HEIGHT(node.height) + ROW_GAP);
            maxHeight = Math.max(maxHeight, node.y + node.height);
            nodePositions.push(node);
        });
    });
    const edges = graph.edges
        .map((edge) => {
        const fromNode = nodePositions.find((n) => n.tableName === edge.from_table);
        const toNode = nodePositions.find((n) => n.tableName === edge.to_table);
        if (!fromNode || !toNode)
            return null;
        const fromPoint = { x: fromNode.x + fromNode.width, y: fromNode.y + fromNode.height / 2 };
        const toPoint = { x: toNode.x, y: toNode.y + toNode.height / 2 };
        return { from: fromNode, to: toNode, fromPoint, toPoint };
    })
        .filter(Boolean);
    const width = Math.max(TYPE_ORDER.length * (CARD_WIDTH + COLUMN_GAP), 0);
    return { nodes: nodePositions, edges, width, height: Math.max(maxHeight, 0) };
}
export const ERDiagram = ({ sql, diagram, graph }) => {
    const parsedGraph = useMemo(() => {
        return graph?.nodes?.length ? graph : normalizeDiagram(diagram) || parseSqlDiagram(sql);
    }, [graph, diagram, sql]);
    if (!parsedGraph || !parsedGraph.nodes.length) {
        return (_jsx("div", { className: "bg-white border border-gray-200 rounded-xl p-6 shadow-sm mb-6 min-h-[200px]", children: _jsx("div", { className: "text-sm text-gray-500", children: "ER diagram will appear here once the model is generated." }) }));
    }
    const layout = buildLayout(parsedGraph);
    return (_jsxs("div", { className: "bg-gradient-to-br from-white to-slate-50 border border-gray-200 rounded-2xl p-6 shadow-lg mb-6", children: [_jsx("div", { className: "flex items-center justify-between mb-4", children: _jsx("h4", { className: "text-lg font-semibold text-gray-900", children: "Dimensional Model ER Diagram" }) }), _jsx("div", { className: "overflow-auto", children: _jsxs("svg", { width: layout.width + 80, height: layout.height + 40, viewBox: `0 0 ${layout.width + 80} ${layout.height + 40}`, children: [_jsx("defs", { children: _jsx("marker", { id: "arrow", viewBox: "0 0 6 6", refX: "5", refY: "3", markerWidth: "6", markerHeight: "6", orient: "auto", children: _jsx("path", { d: "M0,0 L6,3 L0,6 Z", fill: "#2563EB" }) }) }), layout.edges.map((edge, idx) => (_jsx("path", { d: `M${edge.fromPoint.x} ${edge.fromPoint.y} C${edge.fromPoint.x + 40} ${edge.fromPoint.y}, ${edge.toPoint.x - 40} ${edge.toPoint.y}, ${edge.toPoint.x} ${edge.toPoint.y}`, stroke: "#2563EB", strokeWidth: 2, fill: "none", markerEnd: "url(#arrow)", opacity: 0.85 }, `edge-${idx}`))), layout.nodes.map((node) => (_jsxs("g", { children: [_jsx("foreignObject", { x: node.x, y: node.y, width: node.width, height: node.height, children: _jsxs("div", { className: "h-full w-full bg-white ring-1 ring-slate-200 rounded-2xl shadow-sm p-4 flex flex-col", children: [_jsxs("div", { className: "flex items-center justify-between mb-3", children: [_jsx("span", { className: "text-sm font-semibold text-slate-900", children: node.tableName }), _jsx("span", { className: `px-2 py-0.5 text-[10px] font-semibold uppercase rounded-full ${node.type === 'dimension'
                                                            ? 'bg-blue-100 text-blue-700'
                                                            : node.type === 'fact'
                                                                ? 'bg-emerald-100 text-emerald-700'
                                                                : 'bg-slate-100 text-slate-600'}`, children: node.type })] }), _jsx("div", { className: "space-y-1 text-[12px] text-slate-600 flex-1", children: node.columns.map((column) => (_jsxs("div", { className: "flex items-center gap-2", children: [_jsx("span", { className: "font-mono", children: column.name }), column.data_type && _jsx("span", { className: "text-xs text-slate-400", children: column.data_type }), column.is_primary_key && (_jsx("span", { className: "text-xs uppercase text-blue-600 font-semibold", children: "PK" })), column.is_foreign_key && (_jsx("span", { className: "text-xs uppercase text-emerald-600 font-semibold", children: "FK" }))] }, `${node.id}-${column.name}`))) })] }) }), _jsx("rect", { x: node.x, y: node.y, width: node.width, height: node.height, rx: 24, ry: 24, fill: "transparent" })] }, node.id)))] }) })] }));
};
