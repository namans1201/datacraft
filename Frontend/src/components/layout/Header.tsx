import React from 'react';
import { User, LogOut } from 'lucide-react';
import { Button } from '@/components/common/Button';
import { useUIStore } from '@/store/useUIStore';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '@/store/useAuthStore';
import toast from 'react-hot-toast';

  
export const Header: React.FC = () => {
  const navigate = useNavigate();
  const { toggleChat } = useUIStore();
  const logout = useAuthStore((s) => s.logout);

  const handleLogout = () => {
    logout();
    toast.success('Logged out successfully');
    navigate('/login', { replace: true });
  };

  return (
    <header className="bg-white shadow-sm border-b border-gray-200 px-6 py-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-2xl">⚡</span>
          <h1 className="text-2xl font-bold text-primary-600">Navisphere - Agentic Data Ingestion</h1>
        </div>

        <div className="flex items-center gap-4">
          {/* Logout Button */}
          <Button
            variant="primary"
            size="sm"
            icon={<LogOut className="w-4 h-4" />}
            onClick={handleLogout}>
            Logout
          </Button>
          <div className="flex items-center gap-2 px-3 py-2 bg-gray-100 rounded-lg">
            <User className="w-4 h-4 text-gray-600" />
            <span className="text-sm text-gray-700">User Profile</span>
          </div>
        </div>
      </div>
    </header>
  );
};