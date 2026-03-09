import { apiClient } from './client';

export const kpiApi = {
  async analyzeSchema(catalog: string, schema_name: string) {
    return apiClient.post(`/api/kpi/analyze-schema?catalog=${catalog}&schema_name=${schema_name}`);
  },

  async generateKPIs(domain: string, area: string, catalog: string, schema_name: string) {
    return apiClient.post('/api/kpi/generate', { 
      domain, 
      area, 
      catalog, 
      schema_name 
    });
  },
};