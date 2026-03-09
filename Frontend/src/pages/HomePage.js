import { jsx as _jsx } from "react/jsx-runtime";
import { MainLayout } from '@/components/layout/MainLayout';
import { SetupUpload } from '@/components/steps/Step1/SetupUpload';
import { DataLakeDesign } from '@/components/steps/Step2/DataLakeDesign';
import { BusinessKPIs } from '@/components/steps/Step3/BusinessKPIs';
import { CodeGeneration } from '@/components/steps/Step4/CodeGeneration';
import { DataModeling } from '@/components/steps/Step5/DataModeling';
import { useAgentStore } from '@/store/useAgentStore';
export const HomePage = () => {
    const { currentStep } = useAgentStore();
    const renderStep = () => {
        switch (currentStep) {
            case 1:
                return _jsx(SetupUpload, {});
            case 2:
                return _jsx(DataModeling, {});
            case 3:
                return _jsx(DataLakeDesign, {});
            case 4:
                return _jsx(BusinessKPIs, {});
            case 5:
                return _jsx(CodeGeneration, {});
            default:
                return _jsx(SetupUpload, {});
        }
    };
    return _jsx(MainLayout, { children: renderStep() });
};
