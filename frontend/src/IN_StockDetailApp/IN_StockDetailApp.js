import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
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
  AccordionDetails,
  useTheme
} from '@mui/material';
import { LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { format, subDays } from 'date-fns';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ForceIndexGraph from './ForceIndexGraph';
import WilliamsRGraph from './WilliamsRGraph';
import StockPriceGraph from './StockPriceGraph';

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

const formatCurrency = (value) => {
  if (!value) return 'N/A';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2
  }).format(value);
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

const formatRatio = (value) => {
  if (!value) return 'N/A';
  return value.toFixed(2);
};

function US_StockDetailApp() {
  const { symbol } = useParams();
  const [stockData, setStockData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [startDate, setStartDate] = useState(subDays(new Date(), 30));
  const [endDate, setEndDate] = useState(new Date());
  const [expandedPanels, setExpandedPanels] = useState({
    price: true,
    force: true,
    williams: true
  });
  const theme = useTheme();

  useEffect(() => {
    const fetchStockData = async () => {
      try {
        setIsLoading(true);
        setError(null);
        
        const formattedStartDate = format(startDate, 'yyyy-MM-dd');
        const formattedEndDate = format(endDate, 'yyyy-MM-dd');
        
        const url = `${API_URL}/in_stocks/${symbol}/historical?start_date=${formattedStartDate}&end_date=${formattedEndDate}`;
        
        const response = await fetch(url);
        
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        setStockData(data);
      } catch (err) {
        console.error('Error fetching data:', err);
        setError(err.message);
      } finally {
        setIsLoading(false);
      }
    };

    fetchStockData();
  }, [symbol, startDate, endDate]);

  const handlePanelChange = (panel) => (event, isExpanded) => {
    setExpandedPanels(prev => ({
      ...prev,
      [panel]: isExpanded
    }));
  };

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

  const currentData = stockData?.current_data || {};

  return (
    <LocalizationProvider dateAdapter={AdapterDateFns}>
      <Box sx={{ p: 3 }}>
        <Paper sx={{ p: 3, mb: 3 }}>
          {/* Header Section */}
          <Box sx={{ mb: 3 }}>
            <Typography variant="h4" gutterBottom>
              {symbol} - {stockData?.stock_name}
            </Typography>
            <Typography variant="h5" color="primary" gutterBottom>
              {formatCurrency(currentData.close)}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Data as of: {currentData.datetime ? format(new Date(currentData.datetime), 'PPpp') : 'N/A'}
            </Typography>
          </Box>

          {/* Stats Dashboard */}
          <Grid container spacing={2} sx={{ mb: 3 }}>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="Market Cap" 
                value={formatNumber(currentData.market_cap)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="Day's Range" 
                value={`${formatCurrency(currentData.low)} - ${formatCurrency(currentData.high)}`}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="Volume" 
                value={formatNumber(currentData.volume)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="200 EMA" 
                value={formatCurrency(currentData.ema)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="P/E Ratio" 
                value={formatRatio(currentData.pe_ratio)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="EV/EBITDA" 
                value={formatRatio(currentData.ev_ebitda)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="P/B Ratio" 
                value={formatRatio(currentData.pb_ratio)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <StatCard 
                title="PEG Ratio" 
                value={formatRatio(currentData.peg_ratio)}
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
            <Grid item xs={12} sm={6} md={4}>
              <StatCard 
                title="12M Price Change" 
                value={formatPercent(currentData.price_change_12m)}
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
                <StockPriceGraph data={stockData?.price_history || []} />
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
                  Crossovers between the 7-week and 52-week EMAs can signal potential trend changes.
                </Typography>
                <ForceIndexGraph data={stockData?.technical_data || []} />
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
                <WilliamsRGraph data={stockData?.technical_data || []} />
              </AccordionDetails>
            </Accordion>
          </Box>
        </Paper>
      </Box>
    </LocalizationProvider>
  );
}

export default US_StockDetailApp;