import { apiClient } from './client';

export const mappingApi = {
  async generateSilverMappings(
    standard: string,
    catalog: string,
    schema_name: string,
    dbfs_path: string,
    customSchemaFile?: File
  ) {
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

   

  async generateGoldMappings(catalog: string, schema: string) {
    return apiClient.post(
      `/api/mapping/run-gold-mapping?catalog=${catalog}&schema=${schema}`
    );
  },

  async updateMappings(mappings: unknown[]) {
    return apiClient.post('/api/mapping/update', { mappings });
  },

  async rerunRAGForColumns(df_name: string, col_names: string[], standard: string) {
    return apiClient.post('/api/mapping/rerun-rag', {
      df_name,
      col_names,
      standard,
    });
  },
};
