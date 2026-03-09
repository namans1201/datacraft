import React from 'react';
import { ChevronLeft, ChevronRight, Save } from 'lucide-react';
import { Button } from '@/components/common/Button';
import { useAgentStore } from '@/store/useAgentStore';
import { Step } from '../../types/ui';
import toast from 'react-hot-toast';

export const Footer: React.FC = () => {
  const { currentStep, setCurrentStep } = useAgentStore();

  const handlePrevious = () => {
    if (currentStep > 1) {
      setCurrentStep((currentStep - 1) as Step);
    }
  };

  const handleNext = () => {
    if (currentStep < 5) {
      setCurrentStep((currentStep + 1) as Step);
    } else {
      toast.success('Pipeline completed!');
    }
  };

  const handleSave = () => {
    toast.success('Progress saved successfully');
  };

  return (
    <footer className="bg-white border-t border-gray-200 px-6 py-4">
      <div className="flex items-center justify-between">
        <Button
          variant="secondary"
          onClick={handlePrevious}
          disabled={currentStep === 1}
          icon={<ChevronLeft className="w-4 h-4" />}
        >
          Previous Step
        </Button>

        <Button
          variant="ghost"
          onClick={handleSave}
          icon={<Save className="w-4 h-4" />}
        >
          Save Progress
        </Button>

        <Button
          variant="primary"
          onClick={handleNext}
          icon={<ChevronRight className="w-4 h-4" />}
        >
          {currentStep === 5 ? 'Finish' : 'Next Step'}
        </Button>
      </div>
    </footer>
  );
};