export interface CreateCatalogRequest {
  catalog: string;
  token: string;
}


export interface CreateSchemaRequest {
  catalog: string;
  schema_name: string,
  token: string;
}

export interface CreateVolumeRequest {
  catalog: string;
  schema_name: string,
  volume : string,
  token: string;
}

export interface FileUploadRequest {
  file: File;
  catalog: string;
  schema: string;
  volume: string;
  token: string;
}

export interface GenerateMappingRequest {
  standard: 'fhir' | 'acord' | 'x12' | 'aids' | 'custom';
  custom_schema_file?: File;
}

export interface GenerateKPIRequest {
  domain: string;
  area: string;
}

export interface GenerateCodeRequest {
  catalog: string;        
  schema_name: string;    
  include_masking: boolean;
  pii_access_mode: 'group' | 'user';
  pii_access_value: string;
  phi_access_mode: 'group' | 'user';
  phi_access_value: string;
}

export interface Message {
  role: 'user' | 'assistant' | 'system';
  content: string;
}

export interface ChatApiMessage {
  role: 'user' | 'assistant';
  content: string;
}

export interface ChatRequest {
  message: string;
  conversation_history: ChatApiMessage[];
}

export interface ApiResponse<T = unknown> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}
