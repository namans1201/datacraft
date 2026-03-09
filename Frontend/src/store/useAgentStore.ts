import { create } from 'zustand';
import { AgentState, GoldMappingRow, MappingRow, Message } from '../types/agent-state';
import { Step } from '../types/ui';

interface AgentStore extends AgentState {
  // UI State
  currentStep: Step;
  isLoading: boolean;
  error: string | null;
  
  // Actions
  setCurrentStep: (step: Step) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  updateState: (updates: Partial<AgentState>) => void;
  resetState: () => void;
  
  // Step-specific actions
  setCatalogInfo: (catalog: string, schema: string, volume: string) => void;
  setDataFrames: (
    dfs: Record<string, unknown>,
    df_heads: Record<string, unknown>,
    df_dtypes: Record<string, Record<string, string>>
  ) => void;
  setMappings: (mappings: MappingRow[]) => void;
  setGoldMappings: (mappings: GoldMappingRow[]) => void;
  setKPIs: (domain: string, area: string, kpis: string) => void;
  setPySparkCode: (code: string) => void;
  setMaskingSQL: (sql: string) => void;
  setModelingSQL: (sql: string) => void;
  addChatMessage: (message: Message) => void;
}

const initialState: AgentState = {
  catalog: '',
  schema: '',
  volume: '',
  dbfs_path: '',
  dfs: {},
  df_heads: {},
  df_dtypes: {},
  file_types: {},
  xml_root_tags: {},
  mapping_rows: [],
  fhir_mapping_rows: [],
  gold_mapping_rows: [],
  llm_summaries: {},
  col_summaries: {},
  rag_evidence: {},
  domain: null,
  area: null,
  suggested_areas: [],
  kpis: null,
  pii_columns: [],
  phi_columns: [],
  sensitive_metadata: {},
  pyspark_code: '',
  dq_rules: '',
  masking_sql: null,
  masking_sql_lines: [],
  mask_execution_status: 'NOT_STARTED',
  mask_execution_log: [],
  modeling_sql: '',
  modeling_schema_view: '',
  modeling_er_diagram: { tables: [], relationships: [] },
  modeling_diagram: { nodes: [], edges: [] },
  messages: [],
  ui_chat_history: [],
  pii_access_mode: 'group',
  pii_access_value: 'pii_access',
  phi_access_mode: 'group',
  phi_access_value: 'phi_access',
};

export const useAgentStore = create<AgentStore>((set) => ({
  ...initialState,
  currentStep: 1,
  isLoading: false,
  error: null,

  setCurrentStep: (step) => set({ currentStep: step }),
  
  setLoading: (loading) => set({ isLoading: loading }),
  
  setError: (error) => set({ error }),
  
  updateState: (updates) => set((state) => ({ ...state, ...updates })),
  
  resetState: () => set({ ...initialState, currentStep: 1, isLoading: false, error: null }),
  
  setCatalogInfo: (catalog, schema, volume) => set({ catalog, schema, volume }),
  
  setDataFrames: (dfs, df_heads, df_dtypes) => set((state) => ({ 
    ...state,
    dfs: { ...dfs },
    df_heads: { ...df_heads }, 
    df_dtypes: { ...df_dtypes } 
  })),
    
  setMappings: (mappings) => set({ 
    mapping_rows: mappings,
    fhir_mapping_rows: mappings 
  }),
  
  setGoldMappings: (mappings) => set({ gold_mapping_rows: mappings }),
  
  setKPIs: (domain, area, kpis) => set({ domain, area, kpis }),
  
  setPySparkCode: (code) => set({ pyspark_code: code }),
  
  setMaskingSQL: (sql) => set({ masking_sql: sql }),
  
  setModelingSQL: (sql) => set({ modeling_sql: sql }),
  
  addChatMessage: (message) => set((state) => ({
    messages: [...state.messages, message],
    ui_chat_history: [...state.ui_chat_history, message]
  })),
}));
