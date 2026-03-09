import { apiClient } from './client';
import {  Message } from '../types/api';

export const chatApi = {
  async sendMessage(
    message: string, 
    conversation_history: Message[], 
    catalog: string | null, 
    schema_name: string | null
  ) {
    return apiClient.post<{ response: string; agent: string }>('/api/chat/message', {
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