import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import { HomePage } from './pages/HomePage';
import { LoginPage } from './components/auth/LoginPage';
import './styles/globals.css';
function App() {
    return (_jsxs(Router, { children: [_jsx(Toaster, { position: "top-right", toastOptions: {
                    duration: 4000,
                    style: {
                        background: '#fff',
                        color: '#1a202c',
                        boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
                    },
                    success: {
                        iconTheme: {
                            primary: '#10b981',
                            secondary: '#fff',
                        },
                    },
                    error: {
                        iconTheme: {
                            primary: '#ef4444',
                            secondary: '#fff',
                        },
                    },
                } }), _jsxs(Routes, { children: [_jsx(Route, { path: "/login", element: _jsx(LoginPage, {}) }), _jsx(Route, { path: "/home", element: _jsx(HomePage, {}) }), _jsx(Route, { path: "/", element: _jsx(Navigate, { to: "/login", replace: true }) })] })] }));
}
export default App;
{ /* <Routes>
  <Route path="/login" element={<LoginPage />} />

  <Route
    path="/"
    element={
      <ProtectedRoute>
        <MainLayout>
          <HomePage />
        </MainLayout>
      </ProtectedRoute>
    }
  />
</Routes> */
}
