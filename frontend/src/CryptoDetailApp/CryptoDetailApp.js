import React, { useState, useEffect } from 'react';
import { useParams, useLocation } from 'react-router-dom';
import { 
  Box, 
  Typography, 
  Paper, 
  CircularProgress,
  Alert,
  Grid,
  Card,
  CardContent,
  Accordion,
  AccordionSummary,
  AccordionDetails
} from '@mui/material';
import { LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { format, subDays } from 'date-fns';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ForceIndexGraph from './ForceIndexGraph';
import WilliamsRGraph from './WilliamsRGraph';
import CryptoPriceGraph from './CryptoPriceGraph';
import HeikinAshiGraph from './HeikinAshiGraph';

const API_URL = process.env.REACT_APP_API_URL || '/api';

const StatCard = ({ title, value, suffix = '', prefix = '' }) => (
  <Card sx={{ height: '100%' }}>
    <CardContent>
      <Typography color="textSecondary" gutterBottom variant="body2">
        {title}
      </Typography>
      <Typography variant="h6">
        {prefix}{value !== null && value !== undefined ? value : 'N/A'}{suffix}
      </Typography>
    </CardContent>
  </Card>
);

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

const formatPercent = (value) => {
  if (!value) return 'N/A';
  return `${(value * 100).toFixed(2)}%`;
};

const formatNumber = (value) => {
  if (!value) return 'N/A';
  return new Intl.NumberFormat('en-US', {
    notation: 'compact',
    compactDisplay: 'short'
  }).format(value);
};

function CryptoDetailApp() {
  const { symbol } = useParams();
  const location = useLocation();
  const [cryptoData, setCryptoData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [startDate, setStartDate] = useState(subDays(new Date(), 30));
  const [endDate, setEndDate] = useState(new Date());
  const [expandedPanels, setExpandedPanels] = useState({
    price: true,
    force: true,
    williams: true,
    heikinAshi: true
  });

  // Determine base currency from URL path
  const base = location.pathname.includes('_eth') ? 'ETH' : 
               location.pathname.includes('_btc') ? 'BTC' : 'USD';

  const handlePanelChange = (panel) => (event, isExpanded) => {
    setExpandedPanels(prev => ({
      ...prev,
      [panel]: isExpanded
    }));
  };

  useEffect(() => {
    const fetchCryptoData = async () => {
      try {
        setIsLoading(true);
        setError(null);
        
        const formattedStartDate = format(startDate, 'yyyy-MM-dd');
        const formattedEndDate = format(endDate, 'yyyy-MM-dd');
        
        const endpointSuffix = base === 'ETH' ? '/historical_eth' :
                              base === 'BTC' ? '/historical_btc' :
                              '/historical';
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

  const currentData = cryptoData?.current_data || {};

  return (
    <LocalizationProvider dateAdapter={AdapterDateFns}>
      <Box sx={{ p: 3 }}>
        <Paper sx={{ p: 3, mb: 3 }}>
          {/* Header Section */}
          <Box sx={{ mb: 3 }}>
            <Typography variant="h4" gutterBottom>
              {symbol} - {cryptoData?.crypto_name}
            </Typography>
            <Typography variant="h5" color="primary" gutterBottom>
              {formatValue(currentData.close, base)}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Data as of: {currentData.datetime ? format(new Date(currentData.datetime), 'PPpp') : 'N/A'}
            </Typography>
          </Box>

          {/* Stats Dashboard */}
          <Grid container spacing={2} sx={{ mb: 3 }}>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="Volume (24h)" 
                value={formatNumber(currentData.volume)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="Day's Range" 
                value={`${formatValue(currentData.low, base)} - ${formatValue(currentData.high, base)}`}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="200 EMA" 
                value={formatValue(currentData.ema, base)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="All-Time High" 
                value={formatValue(currentData.all_time_high, base)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <StatCard 
                title="% of ATH" 
                value={formatPercent(currentData.ath_percentage)}
                suffix=" from ATH"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <StatCard 
                title="3M Price Change" 
                value={formatPercent(currentData.price_change_3m)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <StatCard 
                title="6M Price Change" 
                value={formatPercent(currentData.price_change_6m)}
              />
            </Grid>
          </Grid>

          {/* Date Filter Section */}
          <Box sx={{ 
            display: 'flex', 
            justifyContent: 'flex-start',
            alignItems: 'center',
            gap: 2,
            mb: 3,
            mt: 4,
            borderTop: 1,
            borderColor: 'divider',
            pt: 3
          }}>
            <Typography variant="subtitle1" sx={{ mr: 2 }}>
              Historical Data Range:
            </Typography>
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

          {/* Graphs Section */}
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Accordion 
              expanded={expandedPanels.price}
              onChange={handlePanelChange('price')}
            >
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography variant="h6">Price History</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Typography variant="body2" color="text.secondary" paragraph>
                  Historical price movement with volume and 200-day EMA overlay.
                </Typography>
                <CryptoPriceGraph 
                  data={cryptoData?.price_history || []} 
                  base={base}
                />
              </AccordionDetails>
            </Accordion>

            <Accordion 
              expanded={expandedPanels.force}
              onChange={handlePanelChange('force')}
            >
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography variant="h6">Force Index</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Typography variant="body2" color="text.secondary" paragraph>
                  The Force Index combines price and volume to measure the strength of price movements.
                  Crossovers between the 4-week and 14-week EMAs can signal potential trend changes.
                </Typography>
                <ForceIndexGraph 
                  data={cryptoData?.technical_data || []} 
                  base={base}
                  formatValue={formatValue}
                />
              </AccordionDetails>
            </Accordion>

            <Accordion 
              expanded={expandedPanels.williams}
              onChange={handlePanelChange('williams')}
            >
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography variant="h6">Williams %R</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Typography variant="body2" color="text.secondary" paragraph>
                  Williams %R is a momentum indicator that measures overbought and oversold levels.
                  Values below -50 combined with a Williams %R crossover above its EMA can signal potential buying opportunities.
                </Typography>
                <WilliamsRGraph 
                  data={cryptoData?.technical_data || []} 
                />
              </AccordionDetails>
            </Accordion>
            
            {/* Heikin-Ashi Chart */}
            <Accordion 
              expanded={expandedPanels.heikinAshi}
              onChange={handlePanelChange('heikinAshi')}
            >
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography variant="h6">Heikin-Ashi Chart</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Typography variant="body2" color="text.secondary" paragraph>
                  Heikin-Ashi charts help identify trends by smoothing price action and filtering out market noise.
                  Use the timeframe selector to view different periods. Color changes from red to green may indicate
                  potential bullish reversals, while green to red changes may indicate bearish reversals.
                </Typography>
                <HeikinAshiGraph 
                  symbol={symbol} 
                  base={base}
                />
              </AccordionDetails>
            </Accordion>
          </Box>
        </Paper>
      </Box>
    </LocalizationProvider>
  );
}

export default CryptoDetailApp;