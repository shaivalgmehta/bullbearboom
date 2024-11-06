import React, { useState, useEffect } from 'react';
import { useParams, useLocation } from 'react-router-dom';
import { 
  Box, 
  Typography, 
  Paper, 
  CircularProgress,
  Alert,
  Card,
  CardContent
} from '@mui/material';
import { LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { format, subDays } from 'date-fns';
import ForceIndexGraph from './ForceIndexGraph';
import WilliamsRGraph from './WilliamsRGraph';

const API_URL = process.env.REACT_APP_API_URL || '/api';

const formatValue = (value, base) => {
  if (value === null || value === undefined) return 'N/A';
  
  switch(base) {
    case 'USD':
      return new Intl.NumberFormat('en-US', { 
        style: 'currency', 
        currency: 'USD',
        maximumFractionDigits: 2 
      }).format(value);
    
    case 'ETH':
      return `${Number(value).toFixed(6)} ETH`;
    
    case 'BTC':
      return `${Number(value).toFixed(8)} BTC`;
    
    default:
      return Number(value).toFixed(6);
  }
};

function CryptoDetailApp() {
  const { symbol } = useParams();
  const location = useLocation();
  const [cryptoData, setCryptoData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [startDate, setStartDate] = useState(subDays(new Date(), 30));
  const [endDate, setEndDate] = useState(new Date());

  // Determine base currency from URL path
  const base = location.pathname.includes('_eth') ? 'ETH' : 
               location.pathname.includes('_btc') ? 'BTC' : 'USD';

  // Get appropriate endpoint suffix based on base currency
  const getEndpointSuffix = () => {
    switch(base) {
      case 'ETH':
        return '/historical_eth';
      case 'BTC':
        return '/historical_btc';
      default:
        return '/historical';
    }
  };

  useEffect(() => {
    const fetchCryptoData = async () => {
      try {
        setIsLoading(true);
        setError(null);
        
        const formattedStartDate = format(startDate, 'yyyy-MM-dd');
        const formattedEndDate = format(endDate, 'yyyy-MM-dd');
        
        const endpointSuffix = getEndpointSuffix();
        const url = `${API_URL}/crypto/${symbol}${endpointSuffix}?start_date=${formattedStartDate}&end_date=${formattedEndDate}`;
        
        const response = await fetch(url);
        
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        setCryptoData(data);
      } catch (err) {
        console.error('Error fetching data:', err);
        setError(err.message);
      } finally {
        setIsLoading(false);
      }
    };

    fetchCryptoData();
  }, [symbol, startDate, endDate, base]);

  if (isLoading) {
    return (
      <Box sx={{ 
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center', 
        height: 'calc(100vh - 64px)'
      }}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ p: 3 }}>
        <Alert severity="error">
          Error loading crypto data: {error}
        </Alert>
      </Box>
    );
  }

  return (
    <LocalizationProvider dateAdapter={AdapterDateFns}>
      <Box sx={{ p: 3 }}>
        <Paper sx={{ p: 3, mb: 3 }}>
          <Box sx={{ 
            display: 'flex', 
            justifyContent: 'space-between', 
            alignItems: 'flex-start',
            flexDirection: { xs: 'column', md: 'row' },
            gap: 2,
            mb: 3 
          }}>
            <Box>
              <Typography variant="h4" gutterBottom>
                {symbol} - {cryptoData?.crypto_name}
              </Typography>
              <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                {cryptoData?.stock_name}
              </Typography>
              <Typography variant="body1" color="text.secondary">
                Technical Analysis ({base} Base)
              </Typography>
            </Box>
            <Box sx={{ 
              display: 'flex', 
              gap: 2,
              flexWrap: 'wrap',
              width: { xs: '100%', md: 'auto' }
            }}>
              <DatePicker
                label="Start Date"
                value={startDate}
                onChange={setStartDate}
                maxDate={endDate}
                slotProps={{ textField: { size: "small" } }}
              />
              <DatePicker
                label="End Date"
                value={endDate}
                onChange={setEndDate}
                minDate={startDate}
                maxDate={new Date()}
                slotProps={{ textField: { size: "small" } }}
              />
            </Box>
          </Box>

          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Force Index ({base})
                </Typography>
                <Typography variant="body2" color="text.secondary" paragraph>
                  The Force Index combines price and volume to measure the strength of price movements.
                  Crossovers between the 7-week and 52-week EMAs can signal potential trend changes.
                </Typography>
                <ForceIndexGraph 
                  data={cryptoData?.data || []} 
                  base={base}
                  formatValue={formatValue}
                />
              </CardContent>
            </Card>

            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Williams %R
                </Typography>
                <Typography variant="body2" color="text.secondary" paragraph>
                  Williams %R is a momentum indicator that measures overbought and oversold levels.
                  Values below -50 combined with a Williams %R crossover above its EMA can signal potential buying opportunities.
                </Typography>
                <WilliamsRGraph 
                  data={cryptoData?.data || []} 
                />
              </CardContent>
            </Card>
          </Box>
        </Paper>
      </Box>
    </LocalizationProvider>
  );
}

export default CryptoDetailApp;