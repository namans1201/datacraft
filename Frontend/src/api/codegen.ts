import { apiClient } from './client';
import { GenerateCodeRequest } from '../types/api';

export const codegenApi = {
  async previewDQExpectations(catalog: string, schema: string) {
    return apiClient.get(`/api/codegen/dq-expectations?catalog=${catalog}&schema=${schema}`);
  },
  
  
  async generateMedallionCode(catalog: string, schema: string) {
    return apiClient.post(`/api/codegen/generate-medallion?catalog=${catalog}&schema=${schema}`);
  },

  async generateMaskingSQL(config: GenerateCodeRequest) {
    return apiClient.post('/api/codegen/generate-masking', config);
  },

  async executeMaskingSQL(catalog: string, schema: string) {
    return apiClient.post(`/api/codegen/execute-masking?catalog=${catalog}&schema=${schema}`);
  },
};