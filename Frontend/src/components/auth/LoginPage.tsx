// import React from 'react';
// import { LoginForm } from './LoginForm';
// import { MicrosoftLoginButton } from './MicrosoftLoginButton';
// import { useAuthStore } from '@/store/useAuthStore';
// import { Link, useNavigate } from 'react-router-dom';

// export const LoginPage: React.FC = () => {
//   const login = useAuthStore((s) => s.login);
//   const navigate = useNavigate();

//   const handleLocalLogin = async (email: string, password: string) => {
//     // TEMP mock — backend later
//     login(
//       'mock-token',
//       { email, name: email.split('@')[0] },
//       'local'
//     );

//     navigate('/');
//   };

//   return (
//     <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-gray-50 to-gray-100 px-4">
//       <div className="w-full max-w-md bg-white rounded-2xl shadow-lg border border-gray-200">

// {/* Header */}
// <div className="px-8 pt-8 text-center">
//   <h1 className="text-2xl font-bold text-gray-900">
//     Sign in to Navisphere - Agentic Data Ingestion
//   </h1>
//   <p className="mt-2 text-sm text-gray-500">
//     Welcome back! Please enter your details.
//   </p>
// </div>

//         {/* Form 
//         <div className="px-8 py-6">
//           <LoginForm onSubmit={handleLocalLogin} />
//         </div>

//         {/* Divider 
//         <div className="flex items-center px-8">
//           <div className="flex-grow border-t border-gray-200" />
//           <span className="px-3 text-xs text-gray-400">OR</span>
//           <div className="flex-grow border-t border-gray-200" />
//         </div> */}

//         {/* Microsoft Login */}
//         <div className="px-8 py-6">
//           <MicrosoftLoginButton />
//         </div>

//         {/* Signup Link 
//         <div className="px-8 py-6">
//         <p className="text-sm text-center text-gray-600">
//           Don’t have an account?{' '}
//           <Link
//             to="/signup"
//             className="text-primary-600 hover:text-primary-700 font-medium"
//           >
//             Sign up
//           </Link>
//         </p>
//         </div>*/}


//         {/* Footer */}
//         <div className="px-8 pb-6 text-center">
//           <p className="text-xs text-gray-400">
//             © {new Date().getFullYear()} Navisphere - Agentic Data Ingestion. All rights reserved.
//           </p>
//         </div>
//       </div>
//     </div>
//   );
// };
import { useMsal } from '@azure/msal-react';
import { loginRequest } from '@/msal/msalConfig';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '@/store/useAuthStore';
import { authApi } from '@/api/auth';
import { useState } from 'react';
import toast from 'react-hot-toast';

export const LoginPage = () => {
  const { instance } = useMsal();
  const navigate = useNavigate();
  const login = useAuthStore((s) => s.login);
  const [isLoading, setIsLoading] = useState(false);

  const handleMicrosoftLogin = async () => {
    try {
      setIsLoading(true);
      
      // Step 1: Authenticate with Microsoft via MSAL (Frontend)
      const response = await instance.loginPopup(loginRequest);
      
      if (!response.idToken) {
        throw new Error('Failed to get ID token from Microsoft');
      }

      // Step 2: Send the ID Token to your FastAPI backend
      // We use ID token because it contains the user's identity claims
      const loginResponse = await authApi.loginMicrosoft(response.idToken);

      // Step 3: Store the BACKEND-generated session/token
      login(loginResponse.user, loginResponse.accessToken);

      toast.success('Login successful!');
      navigate('/home', { replace: true });
    } catch (error) {
      // ... error handling
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-gray-50 to-gray-100 px-4">
      <div className="w-full max-w-md bg-white rounded-2xl shadow-lg border border-gray-200">


        {/* Header */}
        <div className="px-8 pt-8 text-center">
          <h1 className="text-2xl font-bold text-gray-900">
            Sign in to Navisphere - Agentic Data Ingestion
          </h1>
          <p className="mt-2 text-sm text-gray-500">
            Welcome back! 
          </p>
        </div>

        {/* Microsoft Login */}
        <div className="px-8 py-6">

          <button
            onClick={handleMicrosoftLogin}
            disabled={isLoading}
            className="w-full flex items-center justify-center gap-3
                 rounded-lg border border-gray-300 bg-white px-4 py-2.5
                 text-sm font-medium text-gray-700
                 hover:bg-gray-50 transition disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isLoading ? (
              <>
                <div className="h-4 w-4 border-2 border-gray-300 border-t-gray-700 rounded-full animate-spin" />
                Signing in...
              </>
            ) : (
              <>
                <img
                  src="https://upload.wikimedia.org/wikipedia/commons/4/44/Microsoft_logo.svg"
                  alt="Microsoft"
                  className="h-4 w-4"
                />
                Continue with Microsoft
              </>
            )}
          </button>
        </div>

        {/* Footer */}
        <div className="px-8 pb-6 text-center">
          <p className="text-xs text-gray-400">
            © {new Date().getFullYear()} Navisphere - Agentic Data Ingestion. All rights reserved.
          </p>
        </div>
      </div>
    </div>
  );
};