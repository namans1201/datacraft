import React from 'react';
import { Bot, X } from 'lucide-react';
import { clsx } from 'clsx';
import { useUIStore } from '@/store/useUIStore';
import { ChatPanel } from './ChatPanel';

export const ChatWidget: React.FC = () => {
  const { isChatOpen, toggleChat } = useUIStore();

  return (
    <>
      {/* Floating Chat Button */}
      <button
        onClick={toggleChat}
        className={clsx(
          'fixed bottom-20 right-11 z-50',
          'w-14 h-14 rounded-full shadow-lg',
          'bg-primary-600 hover:bg-primary-700',
          'flex items-center justify-center',
          'transition-transform hover:scale-105'
        )}
        aria-label="Open chat"
      >
        {isChatOpen ? (
          <X className="w-6 h-6 text-white" />
        ) : (
          <Bot className="w-6 h-6 text-white" />
        )}
      </button>

      {/* Floating Chat Window */}
      {isChatOpen && (
        <div
          className={clsx(
            'fixed bottom-24 right-6 z-50',
            'w-[380px] h-[520px]',
            'bg-white rounded-xl shadow-2xl',
            'border border-gray-200',
            'flex flex-col overflow-hidden'
          )}
        >
          <ChatPanel />
        </div>
      )}
    </>
  );
};
