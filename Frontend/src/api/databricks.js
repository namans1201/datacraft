import { apiClient } from './client';
export const databricksApi = {
    async createCatalog(data) {
        return apiClient.post('/api/databricks/create-catalog', data);
    },
    async createSchema(data) {
        return apiClient.post('/api/databricks/create-schema', data);
    },
    async createVolume(data) {
        return apiClient.post('/api/databricks/create-volume', data);
    },
    async uploadFile(file, catalog, schema, volume, token) {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('catalog', catalog);
        formData.append('schema', schema);
        formData.append('volume', volume);
        formData.append('token', token);
        return apiClient.postFormData('/api/databricks/upload-file', formData);
    },
    async getTableMetadata(catalog, schema, token) {
        return apiClient.post('/api/databricks/metadata', {
            catalog,
            schema,
            token,
        });
    },
    async readFiles(dbfs_path, token, catalog, schema) {
        return apiClient.post('/api/databricks/read-files', {
            dbfs_path: dbfs_path,
            catalog: catalog,
            schema_name: schema,
            token: token
        });
    },
};
