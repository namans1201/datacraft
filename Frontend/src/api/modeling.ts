import { apiClient } from './client';
import { ERDiagram, ERDiagramGraph } from '@/types/agent-state';


interface ModelingResponse {
  success: boolean;
  modeling_sql?: string;
  er_diagram?: ERDiagram;
  diagram?: ERDiagramGraph;
  message?: string;
}

export const modelingApi = {
  async generateDimensionalModel(
    schema_view: 'bronze' | 'silver', 
    catalog: string, 
    schema_name: string
  ) {
    return apiClient.post<ModelingResponse>('/api/modeling/generate', { 
      schema_view,
      catalog,
      schema_name
    });
  },
};
