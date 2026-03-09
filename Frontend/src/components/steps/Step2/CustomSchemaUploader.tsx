import React from 'react';
import { Upload, File, X } from 'lucide-react';

interface CustomSchemaUploaderProps {
  onFileSelect: (file: File | null) => void;
  selectedFile: File | null;
}

export const CustomSchemaUploader: React.FC<CustomSchemaUploaderProps> = ({
  onFileSelect,
  selectedFile,
}) => {
  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    if (files.length > 0) {
      onFileSelect(files[0]);
    }
  };

  const handleRemove = () => {
    onFileSelect(null);
  };

  if (selectedFile) {
    return (
      <div className="flex items-center justify-between p-4 bg-primary-50 border border-primary-200 rounded-lg">
        <div className="flex items-center gap-3">
          <File className="w-5 h-5 text-primary-600" />
          <div>
            <p className="text-sm font-medium text-gray-900">{selectedFile.name}</p>
            <p className="text-xs text-gray-500">
              {(selectedFile.size / 1024).toFixed(2)} KB
            </p>
          </div>
        </div>
        <button
          onClick={handleRemove}
          className="p-1 hover:bg-primary-100 rounded-full transition-colors"
        >
          <X className="w-4 h-4 text-gray-600" />
        </button>
      </div>
    );
  }

  return (
    <label className="block border-2 border-dashed border-gray-300 rounded-lg p-8 text-center cursor-pointer hover:border-primary-400 hover:bg-gray-50 transition-all">
      <input
        type="file"
        className="hidden"
        accept=".csv,.json,.parquet"
        onChange={handleFileInput}
      />
      
      <div className="flex flex-col items-center gap-2">
        <div className="p-3 bg-gray-100 rounded-full">
          <Upload className="w-6 h-6 text-gray-600" />
        </div>
        
        <div>
          <p className="text-sm font-medium text-gray-900">
            Upload Custom Schema
          </p>
          <p className="text-xs text-gray-500 mt-1">
            CSV, JSON, or Parquet format
          </p>
        </div>
      </div>
    </label>
  );
};