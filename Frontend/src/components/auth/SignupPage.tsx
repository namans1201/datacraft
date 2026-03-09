import React, { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { Button } from '@/components/common/Button';
import { Input } from '@/components/common/Input';
import { authApi } from '@/api/auth';
import toast from 'react-hot-toast';

export const SignupPage: React.FC = () => {
  const navigate = useNavigate();

  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  const handleSignup = async () => {
    if (!email || !password || !confirmPassword) {
      toast.error('All fields are required');
      return;
    }

    if (password !== confirmPassword) {
      toast.error('Passwords do not match');
      return;
    }

    setIsLoading(true);

    try {
      await authApi.signup({ email, password });
      toast.success('Account created successfully');
      navigate('/login');
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Signup failed';
      toast.error(message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50 px-4">
      <div className="w-full max-w-sm bg-white p-8 rounded-xl shadow-md">
        <h1 className="text-2xl font-semibold text-center text-gray-900 mb-2">
          Create your account
        </h1>
        <p className="text-sm text-center text-gray-600 mb-6">
          Sign up to start using Navisphere - Agentic Data Ingestion
        </p>

        <div className="space-y-4">
          <Input
            label="Email"
            placeholder="you@company.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />

          <Input
            label="Password"
            type="password"
            placeholder="Create a password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />

          <Input
            label="Confirm Password"
            type="password"
            placeholder="Re-enter password"
            value={confirmPassword}
            onChange={(e) => setConfirmPassword(e.target.value)}
          />

          <Button
            variant="primary"
            className="w-full"
            isLoading={isLoading}
            onClick={handleSignup}
          >
            Sign up
          </Button>
        </div>

        <p className="text-sm text-center text-gray-600 mt-6">
          Already have an account?{' '}
          <Link
            to="/login"
            className="text-primary-600 hover:text-primary-700 font-medium"
          >
            Log in
          </Link>
        </p>
      </div>
    </div>
  );
};
