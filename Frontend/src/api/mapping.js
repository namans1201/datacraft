import { apiClient } from './client';
export const mappingApi = {
    async generateSilverMappings(standard, catalog, schema_name, dbfs_path, customSchemaFile) {
        const formData = new FormData();
        formData.append('standard', standard);
        formData.append('catalog', catalog);
        formData.append('schema_name', schema_name);
        formData.append('dbfs_path', dbfs_path);
        if (standard === 'custom' && customSchemaFile) {
            // Ensure the key 'custom_schema' matches the backend UploadFile parameter name
            formData.append('custom_schema', customSchemaFile);
        }
        // 3. Always use postFormData for this specific endpoint
        return apiClient.postFormData('/api/mapping/generate-silver', formData);
    },
    async generateGoldMappings(catalog, schema) {
        return apiClient.post(`/api/mapping/run-gold-mapping?catalog=${catalog}&schema=${schema}`);
    },
    async updateMappings(mappings) {
        return apiClient.post('/api/mapping/update', { mappings });
    },
    async rerunRAGForColumns(df_name, col_names, standard) {
        return apiClient.post('/api/mapping/rerun-rag', {
            df_name,
            col_names,
            standard,
        });
    },
};
