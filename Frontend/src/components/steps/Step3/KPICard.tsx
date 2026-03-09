import React from 'react';
import { TrendingUp, Info } from 'lucide-react';
import { Card } from '@/components/common/Card';

interface KPICardProps {
  kpi: {
    name: string;
    formula: string;
    description?: string;
    business_context?: string;
  };
}

export const KPICard: React.FC<KPICardProps> = ({ kpi }) => {
  return (
    <Card padding="md" className="hover:shadow-md transition-shadow">
      <div className="flex items-start gap-3">
        <div className="p-2 bg-primary-50 rounded-lg">
          <TrendingUp className="w-5 h-5 text-primary-600" />
        </div>
        
        <div className="flex-1">
          <h4 className="text-base font-semibold text-gray-900 mb-2">
            {kpi.name}
          </h4>
          
          {kpi.description && (
            <p className="text-sm text-gray-600 mb-3">
              {kpi.description}
            </p>
          )}
          
          <div className="bg-gray-50 rounded-lg p-3 mb-3">
            <p className="text-xs font-medium text-gray-500 mb-1">DAX Formula</p>
            <code className="text-xs text-gray-900 font-mono break-all">
              {kpi.formula}
            </code>
          </div>
          
          {kpi.business_context && (
            <div className="flex items-start gap-2 text-sm">
              <Info className="w-4 h-4 text-gray-400 mt-0.5 flex-shrink-0" />
              <p className="text-gray-600">
                <span className="font-medium">Business Context:</span>{' '}
                {kpi.business_context}
              </p>
            </div>
          )}
        </div>
      </div>
    </Card>
  );
};