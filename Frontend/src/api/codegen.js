import { apiClient } from './client';
export const codegenApi = {
    async previewDQExpectations(catalog, schema) {
        return apiClient.get(`/api/codegen/dq-expectations?catalog=${catalog}&schema=${schema}`);
    },
    async generateMedallionCode(catalog, schema) {
        return apiClient.post(`/api/codegen/generate-medallion?catalog=${catalog}&schema=${schema}`);
    },
    async generateMaskingSQL(config) {
        return apiClient.post('/api/codegen/generate-masking', config);
    },
    async executeMaskingSQL(catalog, schema) {
        return apiClient.post(`/api/codegen/execute-masking?catalog=${catalog}&schema=${schema}`);
    },
};
