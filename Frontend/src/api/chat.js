import { apiClient } from './client';
export const chatApi = {
    async sendMessage(message, conversation_history, catalog, schema_name) {
        return apiClient.post('/api/chat/message', {
            message,
            conversation_history,
            catalog,
            schema_name,
        });
    },
    async getSystemAssessment() {
        return apiClient.get('/api/chat/system-assessment');
    },
};
