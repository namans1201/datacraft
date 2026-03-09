import { create } from 'zustand';
export const useUIStore = create((set) => ({
    isSidebarOpen: true,
    isChatOpen: false,
    isMetadataViewerOpen: false,
    activeTab: 'pipeline',
    toggleSidebar: () => set((state) => ({ isSidebarOpen: !state.isSidebarOpen })),
    toggleChat: () => set((state) => ({ isChatOpen: !state.isChatOpen })),
    toggleMetadataViewer: () => set((state) => ({ isMetadataViewerOpen: !state.isMetadataViewerOpen })),
    setActiveTab: (tab) => set({ activeTab: tab }),
}));
