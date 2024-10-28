import React from 'react';
import ReactDOM from 'react-dom';
import { LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import './index.css';
import MainApp from './MainApp';
import reportWebVitals from './reportWebVitals';

ReactDOM.render(
  <React.StrictMode>
    <LocalizationProvider dateAdapter={AdapterDateFns}>
      <MainApp />
    </LocalizationProvider>
  </React.StrictMode>,
  document.getElementById('root')
);

reportWebVitals();