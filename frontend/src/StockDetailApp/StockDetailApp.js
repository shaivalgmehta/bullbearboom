import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
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

function StockDetailApp() {
  const { symbol } = useParams();
  console.log('StockDetailApp rendered with symbol:', symbol); // Add this
  const [stockData, setStockData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [startDate, setStartDate] = useState(subDays(new Date(), 30));
  const [endDate, setEndDate] = useState(new Date());

  useEffect(() => {
    const fetchStockData = async () => {
      try {
        console.log('Fetching data for symbol:', symbol); // Add this
        setIsLoading(true);
        setError(null);
        
        const formattedStartDate = format(startDate, 'yyyy-MM-dd');
        const formattedEndDate = format(endDate, 'yyyy-MM-dd');
        
        const url = `${API_URL}/stocks/${symbol}/historical?start_date=${formattedStartDate}&end_date=${formattedEndDate}`;
        console.log('Fetching from URL:', url); // Add this
        
        const response = await fetch(url);
        
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        console.log('Received data:', data); // Add this
        setStockData(data);
      } catch (err) {
        console.error('Error fetching data:', err); // Add this
        setError(err.message);
      } finally {
        setIsLoading(false);
      }
    };

    fetchStockData();
  }, [symbol, startDate, endDate]);

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
          Error loading stock data: {error}
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
                {symbol} - {stockData?.stock_name}
              </Typography>
              <Typography variant="body1" color="text.secondary">
                Technical Analysis
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
                  Force Index
                </Typography>
                <Typography variant="body2" color="text.secondary" paragraph>
                  The Force Index combines price and volume to measure the strength of price movements.
                  Crossovers between the 7-week and 52-week EMAs can signal potential trend changes.
                </Typography>
                <ForceIndexGraph data={stockData?.data || []} />
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
                <WilliamsRGraph data={stockData?.data || []} />
              </CardContent>
            </Card>
          </Box>
        </Paper>
      </Box>
    </LocalizationProvider>
  );
}

export default StockDetailApp;