import React from 'react';
import { Database, Table, ArrowRight } from 'lucide-react';
import { clsx } from 'clsx';

interface ModelStructureProps {
  dimensions: string[];
  facts: string[];
}

export const ModelStructure: React.FC<ModelStructureProps> = ({ 
  dimensions, 
  facts 
}) => {
  return (
    <div className="grid md:grid-cols-2 gap-6">
      {/* Dimension Tables */}
      <div className="border border-primary-200 rounded-lg p-6 bg-primary-50">
        <div className="flex items-center gap-2 mb-4">
          <Database className="w-5 h-5 text-primary-600" />
          <h4 className="text-base font-semibold text-primary-900">
            Dimension Tables
          </h4>
        </div>
        
        <p className="text-sm text-gray-600 mb-4">
          Descriptive attributes and reference data (who, what, where, when, why)
        </p>

        {dimensions.length === 0 ? (
          <p className="text-sm text-gray-500 italic">No dimension tables found</p>
        ) : (
          <div className="space-y-2">
            {dimensions.map((dim, idx) => (
              <div
                key={idx}
                className="flex items-center gap-2 p-3 bg-white border border-primary-200 rounded-lg hover:shadow-sm transition-shadow"
              >
                <Table className="w-4 h-4 text-primary-600 flex-shrink-0" />
                <span className="text-sm font-mono text-gray-900">{dim}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Fact Tables */}
      <div className="border border-success-200 rounded-lg p-6 bg-success-50">
        <div className="flex items-center gap-2 mb-4">
          <Database className="w-5 h-5 text-success-600" />
          <h4 className="text-base font-semibold text-success-900">
            Fact Tables
          </h4>
        </div>
        
        <p className="text-sm text-gray-600 mb-4">
          Measurable events and metrics (transactions, interactions, measurements)
        </p>

        {facts.length === 0 ? (
          <p className="text-sm text-gray-500 italic">No fact tables found</p>
        ) : (
          <div className="space-y-2">
            {facts.map((fact, idx) => (
              <div
                key={idx}
                className="flex items-center gap-2 p-3 bg-white border border-success-200 rounded-lg hover:shadow-sm transition-shadow"
              >
                <Table className="w-4 h-4 text-success-600 flex-shrink-0" />
                <span className="text-sm font-mono text-gray-900">{fact}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};
