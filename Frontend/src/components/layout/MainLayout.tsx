import React from 'react';
import { Header } from './Header';
import { Stepper } from './Stepper';
import { Sidebar } from './Sidebar';
import { Footer } from './Footer';
// import { ChatPanel } from '../chat/ChatPanel';
import { useUIStore } from '@/store/useUIStore';
import { ChatWidget } from '../chat/ChatWidget';

interface MainLayoutProps {
  children: React.ReactNode;
}

export const MainLayout: React.FC<MainLayoutProps> = ({ children }) => {
  const { isSidebarOpen } = useUIStore();

  return (
    <div className="flex flex-col h-screen bg-gray-50">
      <Header />
      <Stepper />
      
      <div className="flex flex-1 overflow-hidden">
        {isSidebarOpen && <Sidebar />}
        
        <main className="flex-1 overflow-y-auto p-6">
          <div className="max-w-7xl mx-auto">
            {children}
          </div>
        </main>

        {/* {isChatOpen && (
          <div className="w-96 border-l border-gray-200">
            <ChatPanel />
          </div>
        )} */}
      </div>
      <Footer />
      <ChatWidget />
    </div>
  );
};