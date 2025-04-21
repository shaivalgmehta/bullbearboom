import React from 'react';
import { createRoot } from 'react-dom/client';
import { LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import './index.css';
import MainApp from './MainApp';
import reportWebVitals from './reportWebVitals';
import { AuthProvider } from './Authentication/AuthContext';
import { WatchListProvider } from './Watchlist/WatchListContext';

const container = document.getElementById('root');
const root = createRoot(container);

root.render(
  <React.StrictMode>
    <AuthProvider>
      <WatchListProvider>
        <LocalizationProvider dateAdapter={AdapterDateFns}>
          <MainApp />
        </LocalizationProvider>
      </WatchListProvider>
    </AuthProvider>
  </React.StrictMode>
);

reportWebVitals();