import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Header } from './Header';
import { Stepper } from './Stepper';
import { Sidebar } from './Sidebar';
import { Footer } from './Footer';
// import { ChatPanel } from '../chat/ChatPanel';
import { useUIStore } from '@/store/useUIStore';
import { ChatWidget } from '../chat/ChatWidget';
export const MainLayout = ({ children }) => {
    const { isSidebarOpen } = useUIStore();
    return (_jsxs("div", { className: "flex flex-col h-screen bg-gray-50", children: [_jsx(Header, {}), _jsx(Stepper, {}), _jsxs("div", { className: "flex flex-1 overflow-hidden", children: [isSidebarOpen && _jsx(Sidebar, {}), _jsx("main", { className: "flex-1 overflow-y-auto p-6", children: _jsx("div", { className: "max-w-7xl mx-auto", children: children }) })] }), _jsx(Footer, {}), _jsx(ChatWidget, {})] }));
};
