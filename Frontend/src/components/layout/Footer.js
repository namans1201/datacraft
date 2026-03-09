import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { ChevronLeft, ChevronRight, Save } from 'lucide-react';
import { Button } from '@/components/common/Button';
import { useAgentStore } from '@/store/useAgentStore';
import toast from 'react-hot-toast';
export const Footer = () => {
    const { currentStep, setCurrentStep } = useAgentStore();
    const handlePrevious = () => {
        if (currentStep > 1) {
            setCurrentStep((currentStep - 1));
        }
    };
    const handleNext = () => {
        if (currentStep < 5) {
            setCurrentStep((currentStep + 1));
        }
        else {
            toast.success('Pipeline completed!');
        }
    };
    const handleSave = () => {
        toast.success('Progress saved successfully');
    };
    return (_jsx("footer", { className: "bg-white border-t border-gray-200 px-6 py-4", children: _jsxs("div", { className: "flex items-center justify-between", children: [_jsx(Button, { variant: "secondary", onClick: handlePrevious, disabled: currentStep === 1, icon: _jsx(ChevronLeft, { className: "w-4 h-4" }), children: "Previous Step" }), _jsx(Button, { variant: "ghost", onClick: handleSave, icon: _jsx(Save, { className: "w-4 h-4" }), children: "Save Progress" }), _jsx(Button, { variant: "primary", onClick: handleNext, icon: _jsx(ChevronRight, { className: "w-4 h-4" }), children: currentStep === 5 ? 'Finish' : 'Next Step' })] }) }));
};
