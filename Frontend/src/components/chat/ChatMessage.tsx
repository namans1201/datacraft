import React from 'react';
import { Bot, User } from 'lucide-react';
import { clsx } from 'clsx';
import { Message } from '../../types/agent-state';

interface ChatMessageProps {
  message: Message;
}

export const ChatMessage: React.FC<ChatMessageProps> = ({ message }) => {
  const isUser = message.role === 'user';

  return (
    <div className={clsx('flex items-start gap-3', isUser && 'flex-row-reverse')}>
      <div className={clsx(
        'p-2 rounded-lg flex-shrink-0',
        isUser ? 'bg-primary-50' : 'bg-gray-100'
      )}>
        {isUser ? (
          <User className="w-4 h-4 text-primary-600" />
        ) : (
          <Bot className="w-4 h-4 text-gray-600" />
        )}
      </div>
      
      <div className={clsx(
        'flex-1 px-4 py-2 rounded-lg max-w-[80%]',
        isUser 
          ? 'bg-primary-600 text-white'
          : 'bg-gray-100 text-gray-900'
      )}>
        {message.name && !isUser && (
          <p className="text-xs text-gray-500 mb-1">{message.name}</p>
        )}
        <p className="text-sm whitespace-pre-wrap">{message.content}</p>
        {message.timestamp && (
          <p className={clsx(
            'text-xs mt-1',
            isUser ? 'text-primary-100' : 'text-gray-500'
          )}>
            {new Date(message.timestamp).toLocaleTimeString()}
          </p>
        )}
      </div>
    </div>
  );
};