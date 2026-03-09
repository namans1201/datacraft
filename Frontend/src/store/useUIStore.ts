import { create } from 'zustand';

interface UIStore {
  isSidebarOpen: boolean;
  isChatOpen: boolean;
  isMetadataViewerOpen: boolean;
  activeTab: string;
  
  toggleSidebar: () => void;
  toggleChat: () => void;
  toggleMetadataViewer: () => void;
  setActiveTab: (tab: string) => void;
}

export const useUIStore = create<UIStore>((set) => ({
  isSidebarOpen: true,
  isChatOpen: false,
  isMetadataViewerOpen: false,
  activeTab: 'pipeline',
  
  toggleSidebar: () => set((state) => ({ isSidebarOpen: !state.isSidebarOpen })),
  toggleChat: () => set((state) => ({ isChatOpen: !state.isChatOpen })),
  toggleMetadataViewer: () => set((state) => ({ isMetadataViewerOpen: !state.isMetadataViewerOpen })),
  setActiveTab: (tab) => set({ activeTab: tab }),
}));