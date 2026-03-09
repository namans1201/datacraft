/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: {
          50: '#eff6ff',
          100: '#dbeafe',
          500: '#3b82f6',
          600: '#2563eb',
          700: '#1d4ed8',
        },
        success: {
          50: '#d1fae5',
          500: '#10b981',
          700: '#065f46',
        },
        warning: {
          50: '#fef3c7',
          500: '#f59e0b',
          700: '#92400e',
        },
        danger: {
          50: '#fee2e2',
          500: '#ef4444',
          700: '#991b1b',
        },
      },
    },
  },
  plugins: [],
}

