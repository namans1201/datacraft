import { apiClient } from './client';
export const modelingApi = {
    async generateDimensionalModel(schema_view, catalog, schema_name) {
        return apiClient.post('/api/modeling/generate', {
            schema_view,
            catalog,
            schema_name
        });
    },
};
