import React, { useCallback } from 'react';
import { Upload, File } from 'lucide-react';
import { clsx } from 'clsx';

interface FileUploaderProps {
  onFileSelect: (file: File) => void;
}

export const FileUploader: React.FC<FileUploaderProps> = ({ onFileSelect }) => {
  const [isDragging, setIsDragging] = React.useState(false);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);

      const files = Array.from(e.dataTransfer.files);
      if (files.length > 0) {
        onFileSelect(files[0]);
      }
    },
    [onFileSelect]
  );

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    if (files.length > 0) {
      onFileSelect(files[0]);
    }
  };

  return (
    <label
      className={clsx(
        'block border-2 border-dashed rounded-lg p-12 text-center cursor-pointer transition-all',
        isDragging
          ? 'border-primary-500 bg-primary-50'
          : 'border-gray-300 hover:border-primary-400 hover:bg-gray-50'
      )}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <input
        type="file"
        className="hidden"
        accept=".csv,.xlsx,.parquet,.json,.xml"
        onChange={handleFileInput}
      />
      
      <div className="flex flex-col items-center gap-3">
        <div className="p-4 bg-primary-50 rounded-full">
          <Upload className="w-8 h-8 text-primary-600" />
        </div>
        
        <div>
          <p className="text-base font-medium text-gray-900 mb-1">
            Drag & Drop or Click to Upload
          </p>
          <p className="text-sm text-gray-500">
            Supported: CSV, XLSX, Parquet, JSON, XML
          </p>
        </div>
      </div>
    </label>
  );
};