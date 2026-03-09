import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Bot, User } from 'lucide-react';
import { clsx } from 'clsx';
export const ChatMessage = ({ message }) => {
    const isUser = message.role === 'user';
    return (_jsxs("div", { className: clsx('flex items-start gap-3', isUser && 'flex-row-reverse'), children: [_jsx("div", { className: clsx('p-2 rounded-lg flex-shrink-0', isUser ? 'bg-primary-50' : 'bg-gray-100'), children: isUser ? (_jsx(User, { className: "w-4 h-4 text-primary-600" })) : (_jsx(Bot, { className: "w-4 h-4 text-gray-600" })) }), _jsxs("div", { className: clsx('flex-1 px-4 py-2 rounded-lg max-w-[80%]', isUser
                    ? 'bg-primary-600 text-white'
                    : 'bg-gray-100 text-gray-900'), children: [message.name && !isUser && (_jsx("p", { className: "text-xs text-gray-500 mb-1", children: message.name })), _jsx("p", { className: "text-sm whitespace-pre-wrap", children: message.content }), message.timestamp && (_jsx("p", { className: clsx('text-xs mt-1', isUser ? 'text-primary-100' : 'text-gray-500'), children: new Date(message.timestamp).toLocaleTimeString() }))] })] }));
};
