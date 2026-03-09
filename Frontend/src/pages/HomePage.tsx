import React from 'react';
import { MainLayout } from '@/components/layout/MainLayout';
import { SetupUpload } from '@/components/steps/Step1/SetupUpload';
import { DataLakeDesign } from '@/components/steps/Step2/DataLakeDesign';
import { BusinessKPIs } from '@/components/steps/Step3/BusinessKPIs';
import { CodeGeneration } from '@/components/steps/Step4/CodeGeneration';
import { DataModeling } from '@/components/steps/Step5/DataModeling';
import { useAgentStore } from '@/store/useAgentStore';

export const HomePage: React.FC = () => {
  const { currentStep } = useAgentStore();

  const renderStep = () => {
    switch (currentStep) {
      case 1:
        return <SetupUpload />;
      case 2:
        return <DataModeling />;
      case 3:
        return <DataLakeDesign />;
      case 4:
        return <BusinessKPIs />;
      case 5:
        return <CodeGeneration />;
      default:
        return <SetupUpload />;
    }
  };

  return <MainLayout>{renderStep()}</MainLayout>;
};
