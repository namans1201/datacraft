import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useState, useRef, useEffect } from 'react';
import { Send, X, Bot, Loader2 } from 'lucide-react';
import { Button } from '@/components/common/Button';
import { useUIStore } from '@/store/useUIStore';
import { useAgentStore } from '@/store/useAgentStore';
import { chatApi } from '@/api/chat';
import { ChatMessage } from './ChatMessage';
import toast from 'react-hot-toast';
export const ChatPanel = () => {
    const { toggleChat } = useUIStore();
    // UPDATED: Destructure catalog and schema from the store
    const { ui_chat_history, addChatMessage, catalog, schema } = useAgentStore();
    const [message, setMessage] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const messagesEndRef = useRef(null);
    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    };
    useEffect(() => {
        scrollToBottom();
    }, [ui_chat_history]);
    const handleSend = async () => {
        if (!message.trim() || isLoading)
            return;
        // Create the user message object for local UI update
        const userMessage = {
            role: 'user',
            content: message.trim(),
            timestamp: new Date().toISOString(),
        };
        // Add to local store immediately so the user sees their message
        addChatMessage(userMessage);
        const messageToSend = message.trim();
        setMessage('');
        setIsLoading(true);
        try {
            // UPDATED: Pass catalog and schema to the API call
            // This ensures the backend receives the context instead of "None_None"
            const result = await chatApi.sendMessage(messageToSend, ui_chat_history, catalog, schema);
            if (result.success) {
                const assistantMessage = {
                    role: 'assistant',
                    content: result.data?.response || 'No response',
                    name: result.data?.agent || 'Assistant',
                    timestamp: new Date().toISOString(),
                };
                addChatMessage(assistantMessage);
            }
            else {
                toast.error(result.error || 'Failed to send message');
            }
        }
        catch (error) {
            toast.error('An unexpected error occurred');
            console.error(error);
        }
        finally {
            setIsLoading(false);
        }
    };
    const handleKeyPress = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };
    return (_jsxs("div", { className: "flex flex-col h-full bg-white", children: [_jsxs("div", { className: "flex items-center justify-between px-4 py-3 border-b border-gray-200", children: [_jsxs("div", { className: "flex items-center gap-2", children: [_jsx(Bot, { className: "w-5 h-5 text-primary-600" }), _jsx("h3", { className: "text-base font-semibold text-gray-900", children: "Navisphere - Agentic Data Ingestion Agent" })] }), _jsx("button", { onClick: toggleChat, className: "p-1 hover:bg-gray-100 rounded-full transition-colors", children: _jsx(X, { className: "w-5 h-5 text-gray-600" }) })] }), _jsx("div", { className: "flex-1 overflow-y-auto p-4 space-y-4", children: ui_chat_history.length === 0 ? (_jsxs("div", { className: "flex flex-col items-center justify-center h-full text-center px-4", children: [_jsx("div", { className: "p-4 bg-primary-50 rounded-full mb-4", children: _jsx(Bot, { className: "w-8 h-8 text-primary-600" }) }), _jsx("h4", { className: "text-base font-medium text-gray-900 mb-2", children: "Chat with Navisphere - Agentic Data Ingestion Agent" }), _jsx("p", { className: "text-sm text-gray-600 mb-4", children: "Ask questions about your data, mappings, KPIs, or code generation." }), _jsxs("div", { className: "space-y-2 text-left w-full max-w-xs", children: [_jsx("button", { onClick: () => setMessage('What data did I upload?'), className: "w-full text-left px-3 py-2 bg-gray-50 hover:bg-gray-100 rounded-lg text-sm text-gray-700 transition-colors", children: "\"What data did I upload?\"" }), _jsx("button", { onClick: () => setMessage('Explain the silver mappings'), className: "w-full text-left px-3 py-2 bg-gray-50 hover:bg-gray-100 rounded-lg text-sm text-gray-700 transition-colors", children: "\"Explain the silver mappings\"" }), _jsx("button", { onClick: () => setMessage('Show me the generated KPIs'), className: "w-full text-left px-3 py-2 bg-gray-50 hover:bg-gray-100 rounded-lg text-sm text-gray-700 transition-colors", children: "\"Show me the generated KPIs\"" })] })] })) : (_jsxs(_Fragment, { children: [ui_chat_history.map((msg, idx) => (_jsx(ChatMessage, { message: msg }, idx))), isLoading && (_jsxs("div", { className: "flex items-start gap-3", children: [_jsx("div", { className: "p-2 bg-primary-50 rounded-lg", children: _jsx(Bot, { className: "w-4 h-4 text-primary-600" }) }), _jsxs("div", { className: "flex items-center gap-2 px-4 py-2 bg-gray-50 rounded-lg", children: [_jsx(Loader2, { className: "w-4 h-4 animate-spin text-gray-600" }), _jsx("span", { className: "text-sm text-gray-600", children: "Thinking..." })] })] })), _jsx("div", { ref: messagesEndRef })] })) }), _jsx("div", { className: "border-t border-gray-200 p-4", children: _jsxs("div", { className: "flex items-end gap-2", children: [_jsx("textarea", { value: message, onChange: (e) => setMessage(e.target.value), onKeyPress: handleKeyPress, placeholder: "Ask about your data, mappings, or KPIs...", rows: 3, className: "flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm resize-none focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent" }), _jsx(Button, { variant: "primary", size: "md", onClick: handleSend, disabled: !message.trim() || isLoading, icon: _jsx(Send, { className: "w-4 h-4" }) })] }) })] }));
};
