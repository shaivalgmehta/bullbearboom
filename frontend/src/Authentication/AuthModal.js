import React, { useState } from 'react';
import {
  Dialog, DialogTitle, DialogContent, DialogActions,
  TextField, Button, Tab, Tabs, Box
} from '@mui/material';
import { useAuth } from '../hooks/useAuth';

function AuthModal({ open, onClose }) {
  const [tab, setTab] = useState(0);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const { login, register } = useAuth();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');

    try {
      if (tab === 0) {
        await login(email, password);
      } else {
        await register(email, password);
      }
      onClose();
    } catch (err) {
      console.error("Auth error:", err);

      if (err.response) {
        console.log("Status:", err.response.status);
        console.log("Data:", err.response.data);

        if (err.response.status === 409) {
          setError("This email is already registered.");
        } else if (err.response.status === 400) {
          setError(err.response.data.error);
        } else if (err.response.data?.error) {
          setError(err.response.data.error);
        } else {
          setError("Something went wrong. Please try again.");
        }
      } else {
        setError("Network error. Please check your connection.");
      }
    }
  };

  return (
    <Dialog open={open} onClose={onClose}>
      <DialogTitle>
        <Tabs value={tab} onChange={(e, newValue) => setTab(newValue)}>
          <Tab label="Login" />
          <Tab label="Register" />
        </Tabs>
      </DialogTitle>
      <form onSubmit={handleSubmit}>
        <DialogContent>
          <TextField
            label="Email"
            type="email"
            fullWidth
            margin="normal"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
          />
          <TextField
            label="Password"
            type="password"
            fullWidth
            margin="normal"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
          {error && (
            <Box color="error.main" mt={2}>
              {error}
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="contained" color="primary">
            {tab === 0 ? 'Login' : 'Register'}
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
}

export default AuthModal;
