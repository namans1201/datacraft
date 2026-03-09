import { apiClient } from './client';
import { CreateCatalogRequest , CreateSchemaRequest, CreateVolumeRequest } from '../types/api';

export const databricksApi = {
  async createCatalog(data: CreateCatalogRequest) {
    return apiClient.post('/api/databricks/create-catalog', data);
  },

  async createSchema(data: CreateSchemaRequest) {
    return apiClient.post('/api/databricks/create-schema', data);
  },

  async createVolume(data: CreateVolumeRequest) {
    return apiClient.post('/api/databricks/create-volume', data);
  },

  async uploadFile(file: File, catalog: string, schema: string, volume: string, token: string) {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('catalog', catalog);
    formData.append('schema', schema);
    formData.append('volume', volume);
    formData.append('token', token);
    
    return apiClient.postFormData('/api/databricks/upload-file', formData);
  },

  async getTableMetadata(catalog: string, schema: string, token: string) {
    return apiClient.post('/api/databricks/metadata', {
      catalog,
      schema,
      token,
    });
  },

  async readFiles(dbfs_path: string, token: string, catalog: string, schema: string) {
    return apiClient.post('/api/databricks/read-files', { 
      dbfs_path: dbfs_path,
      catalog: catalog,
      schema_name: schema,
      token: token 
    });
  },
};
