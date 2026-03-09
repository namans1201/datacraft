export interface AgentState {
  // Step 1 - Setup & Upload
  catalog: string;
  schema: string;
  volume: string;
  dbfs_path: string;
  dfs: Record<string, unknown>;
  df_heads: Record<string, unknown>;
  df_dtypes: Record<string, Record<string, string>>;
  file_types: Record<string, string>;
  xml_root_tags: Record<string, string>;

  // Step 3 - Data Lake Design
  mapping_rows: MappingRow[];
  fhir_mapping_rows: MappingRow[];
  gold_mapping_rows: GoldMappingRow[];
  llm_summaries: Record<string, string>;
  col_summaries: Record<string, Record<string, string>>;
  rag_evidence: Record<string, Record<string, RAGEvidence>>;

  // Step 4 - Business KPIs
  domain: string | null;
  area: string | null;
  suggested_areas: string[];
  kpis: string | null;

  // Step 5 - Code Generation
  pii_columns: string[];
  phi_columns: string[];
  sensitive_metadata: Record<string, unknown>;
  pyspark_code: string;
  dq_rules: string;
  masking_sql: string | null;
  masking_sql_lines: string[];
  mask_execution_status: 'NOT_STARTED' | 'RUNNING' | 'SUCCESS' | 'FAILED';
  mask_execution_log: MaskExecutionLog[];

  // Step 2 - Data Modeling
  modeling_sql: string;
  modeling_schema_view: string;
  modeling_er_diagram: ERDiagram;
  modeling_diagram: ERDiagramGraph;

  // Chat
  messages: Message[];
  ui_chat_history: Message[];

  // Execution config
  pii_access_mode: 'group' | 'user';
  pii_access_value: string;
  phi_access_mode: 'group' | 'user';
  phi_access_value: string;
}

export interface ERDiagram {
  tables: ERTable[];
  relationships: ERRelationship[];
}

export interface ERTable {
  name: string;
  columns: ERColumn[];
}

export interface ERColumn {
  name: string;
  data_type?: string;
  is_primary_key: boolean;
  is_foreign_key: boolean;
}

export interface ERRelationship {
  from_table: string;
  from_column: string;
  to_table: string;
  to_column: string;
  from_cardinality?: '1' | 'N';
  to_cardinality?: '1' | 'N';
}

export interface DiagramNode {
  table_name: string;
  columns: ERColumn[];
  table_type: 'dimension' | 'fact' | 'table';
}

export interface DiagramEdge {
  from_table: string;
  from_column: string;
  to_table: string;
  to_column: string;
  from_cardinality?: '1' | 'N';
  to_cardinality?: '1' | 'N';
}

export interface ERDiagramGraph {
  nodes: DiagramNode[];
  edges: DiagramEdge[];
}

export interface MappingRow {
  bronze_table: string;
  bronze_columns: string;
  silver_table: string;
  silver_column: string;
  data_classification?: 'PII' | 'PHI' | 'PCI' | 'NON_SENSITIVE';
}

export interface GoldMappingRow {
  silver_table: string;
  silver_column: string;
  gold_table: string;
  gold_column: string;
  transformation?: string;
  description?: string;
  table_type?: 'dimension' | 'fact';
}

export interface RAGEvidence {
  resource_docs: RAGDocument[];
  column_docs: RAGDocument[];
}

export interface RAGDocument {
  resource_name?: string;
  target_table?: string;
  fhir_path?: string;
  target_column?: string;
  description?: string;
  score: number | null;
  text: string;
}

export interface Message {
  role: 'user' | 'assistant' | 'system';
  content: string;
  name?: string;
  timestamp?: string;
}

export interface MaskExecutionLog {
  timestamp: string;
  level: 'info' | 'warning' | 'error';
  message: string;
}
