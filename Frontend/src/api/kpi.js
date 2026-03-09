import { apiClient } from './client';
export const kpiApi = {
    async analyzeSchema(catalog, schema_name) {
        return apiClient.post(`/api/kpi/analyze-schema?catalog=${catalog}&schema_name=${schema_name}`);
    },
    async generateKPIs(domain, area, catalog, schema_name) {
        return apiClient.post('/api/kpi/generate', {
            domain,
            area,
            catalog,
            schema_name
        });
    },
};
