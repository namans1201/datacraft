export type Step = 1 | 2 | 3 | 4 | 5;

export interface StepConfig {
  id: Step;
  label: string;
  description: string;
  icon: string;
}

export interface DataClassification {
  type: 'PII' | 'PHI' | 'PCI' | 'NON_SENSITIVE';
  color: string;
  bgColor: string;
}

export interface TableType {
  type: 'dimension' | 'fact';
  color: string;
  bgColor: string;
}

export interface DQExpectation {
  rule_name: string;
  condition: string;
  enforcement: 'FAIL' | 'DROP' | 'LOG';
  table: string;
}

export interface KPICard {
  name: string;
  description: string;
  formula: string;
  business_context: string;
  category?: string;
}