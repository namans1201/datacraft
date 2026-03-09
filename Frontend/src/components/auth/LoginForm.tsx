import React, { useState } from 'react';
import { Button } from '@/components/common/Button';

interface LoginFormProps {
  onSubmit: (email: string, password: string) => void;
}

export const LoginForm: React.FC<LoginFormProps> = ({ onSubmit }) => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        onSubmit(email, password);
      }}
      className="space-y-4"
    >
      <div>
        <input
          type="email"
          placeholder="Email address"
          required
          className="w-full rounded-lg border border-gray-300 px-4 py-2.5 text-sm
                     focus:border-primary-500 focus:ring-2 focus:ring-primary-100 outline-none"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />
      </div>

      <div>
        <input
          type="password"
          placeholder="Password"
          required
          className="w-full rounded-lg border border-gray-300 px-4 py-2.5 text-sm
                     focus:border-primary-500 focus:ring-2 focus:ring-primary-100 outline-none"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
      </div>

      <Button
        type="submit"
        variant="primary"
        className="w-full"
      >
        Sign In
      </Button>
    </form>
  );
};
