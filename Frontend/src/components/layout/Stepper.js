import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import React from 'react';
import { Check } from 'lucide-react';
import { clsx } from 'clsx';
import { useAgentStore } from '@/store/useAgentStore';
const steps = [
    { id: 1, label: 'Setup & Upload' },
    { id: 2, label: 'Data Modeling' },
    { id: 3, label: 'Data Lake Design' },
    { id: 4, label: 'Business KPIs' },
    { id: 5, label: 'Code Generation' },
];
export const Stepper = () => {
    const { currentStep, setCurrentStep } = useAgentStore();
    return (_jsx("div", { className: "bg-white shadow-sm border-b border-gray-200 px-6 py-6", children: _jsx("div", { className: "max-w-4xl mx-auto", children: _jsx("div", { className: "flex items-center justify-between", children: steps.map((step, idx) => {
                    const isActive = currentStep === step.id;
                    const isCompleted = currentStep > step.id;
                    return (_jsxs(React.Fragment, { children: [_jsxs("div", { className: "flex flex-col items-center relative", children: [_jsx("button", { onClick: () => setCurrentStep(step.id), className: clsx('w-10 h-10 rounded-full flex items-center justify-center font-semibold transition-all', isCompleted && 'bg-success-500 text-white', isActive && 'bg-primary-600 text-white ring-4 ring-primary-100', !isActive && !isCompleted && 'bg-gray-200 text-gray-600'), children: isCompleted ? _jsx(Check, { className: "w-5 h-5" }) : step.id }), _jsx("span", { className: clsx('text-xs mt-2 text-center whitespace-nowrap', isActive && 'text-primary-600 font-semibold', !isActive && 'text-gray-600'), children: step.label })] }), idx < steps.length - 1 && (_jsx("div", { className: clsx('flex-1 h-0.5 mx-4 mt-0', isCompleted ? 'bg-success-500' : 'bg-gray-200') }))] }, step.id));
                }) }) }) }));
};
