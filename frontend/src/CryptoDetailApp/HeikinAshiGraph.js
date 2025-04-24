import React, { useState, useEffect } from 'react';
import {
  ResponsiveContainer,
  ComposedChart,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  Bar,
  Line
} from 'recharts';
import {
  Box,
  Paper,
  Typography,
  CircularProgress,
  Select,
  MenuItem,
  FormControl,
  InputLabel
} from '@mui/material';
import { format, parseISO } from 'date-fns';
import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || '/api';

const HeikinAshiGraph = ({ symbol, base = 'USD' }) => {
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [heikinAshiData, setHeikinAshiData] = useState([]);
  const [colorChanges, setColorChanges] = useState([]);
  const [timeframe, setTimeframe] = useState('3d');

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        setError(null);

        // Determine the appropriate endpoint based on the base currency
        let endpoint = `/crypto/${symbol}/heikin-ashi`;
        if (base === 'ETH') {
          endpoint = `/crypto/${symbol}/heikin-ashi-eth`;
        } else if (base === 'BTC') {
          endpoint = `/crypto/${symbol}/heikin-ashi-btc`;
        }

        const response = await axios.get(
          `${API_URL}${endpoint}?timeframe=${timeframe}`
        );

        if (!response.data.ha_data || response.data.ha_data.length === 0) {
          setError('No Heikin-Ashi data available for this timeframe');
          setIsLoading(false);
          return;
        }

        const formattedData = response.data.ha_data.map((candle, index) => ({
          ...candle,
          formattedDateTime: format(parseISO(candle.datetime), 'MMM d, yyyy'),
          isBullish: candle.ha_close > candle.ha_open,
          bodyTop: Math.max(candle.ha_open, candle.ha_close),
          bodyBottom: Math.min(candle.ha_open, candle.ha_close),
          wickHigh: candle.ha_high,
          wickLow: candle.ha_low,
        }));

        const changes = response.data.color_changes || [];
        const formattedChanges = changes.map(change => ({
          ...change,
          formattedDate: format(parseISO(change.datetime), 'MMM d, yyyy')
        }));

        setHeikinAshiData(formattedData);
        setColorChanges(formattedChanges);
        setIsLoading(false);
      } catch (err) {
        console.error('Error fetching Heikin-Ashi data:', err);
        setError('Failed to load Heikin-Ashi data');
        setIsLoading(false);
      }
    };

    if (symbol) {
      fetchData();
    }
  }, [symbol, timeframe, base]);

  const handleTimeframeChange = (event) => {
    setTimeframe(event.target.value);
  };

  const formatDate = (dateStr) => {
    return format(parseISO(dateStr), 'MMM d, yyyy');
  };

  const formatValue = (value) => {
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

  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const item = payload[0].payload;
      return (
        <Paper sx={{ p: 2 }}>
          <Typography variant="body2">{item.formattedDateTime}</Typography>
          <Typography variant="body2">Open: {formatValue(item.ha_open)}</Typography>
          <Typography variant="body2">High: {formatValue(item.ha_high)}</Typography>
          <Typography variant="body2">Low: {formatValue(item.ha_low)}</Typography>
          <Typography variant="body2">Close: {formatValue(item.ha_close)}</Typography>
          <Typography variant="body2">
            Type: {item.isBullish ? 'Bullish' : 'Bearish'}
          </Typography>
        </Paper>
      );
    }
    return null;
  };

  const timeframeOptions = [
    { value: '3d', label: '3-Day' },
    { value: '2w', label: '2-Week' }
  ];

  const allHighs = heikinAshiData.map(d => d.ha_high);
  const allLows = heikinAshiData.map(d => d.ha_low);
  const minValue = Math.min(...allLows) * 0.99;
  const maxValue = Math.max(...allHighs) * 1.01;

  return (
    <Box sx={{ width: '100%' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel id="timeframe-select-label">Timeframe</InputLabel>
          <Select
            labelId="timeframe-select-label"
            value={timeframe}
            label="Timeframe"
            onChange={handleTimeframeChange}
          >
            {timeframeOptions.map(option => (
              <MenuItem key={option.value} value={option.value}>
                {option.label}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>

      {isLoading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', height: 400 }}>
          <CircularProgress />
        </Box>
      ) : error ? (
        <Typography color="error">{error}</Typography>
      ) : (
        <Box sx={{ width: '100%', height: 400 }}>
          <ResponsiveContainer>
            <ComposedChart
              data={heikinAshiData}
              margin={{ top: 10, right: 30, bottom: 70 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="datetime"
                tickFormatter={formatDate}
                angle={-45}
                textAnchor="end"
                height={80}
                interval="preserveStartEnd"
                minTickGap={50}
                type="category"
              />
              <YAxis
                domain={[minValue, maxValue]}
                tickCount={8}
                tickFormatter={(value) => {
                  // Simplify tick formatting based on the base currency
                  if (base === 'USD') {
                    return new Intl.NumberFormat('en-US', {
                      style: 'currency',
                      currency: 'USD',
                      notation: 'compact',
                      maximumFractionDigits: 2
                    }).format(value);
                  } else if (base === 'ETH') {
                    return `${Number(value).toFixed(4)} ETH`;
                  } else if (base === 'BTC') {
                    return `${Number(value).toFixed(6)} BTC`;
                  }
                  return value.toFixed(6);
                }}
              />
              <Tooltip content={<CustomTooltip />} />

              {/* Wicks as lines */}
              <Line
                type="monotone"
                dataKey="wickHigh"
                stroke="transparent"
                dot={false}
                activeDot={false}
                isAnimationActive={false}
              />
              {heikinAshiData.map((d, i) => (
                <line
                  key={i}
                  x1={`${(i + 0.5) * (100 / heikinAshiData.length)}%`}
                  x2={`${(i + 0.5) * (100 / heikinAshiData.length)}%`}
                  y1="0"
                  y2="100%"
                  stroke="transparent"
                  style={{
                    position: 'absolute',
                    pointerEvents: 'none',
                    zIndex: 0
                  }}
                />
              ))}

              {/* Candle bodies */}
              <Bar
                dataKey="bodyTop"
                fill="green"
                barSize={8}
                shape={(props) => {
                  const { x, y, width, height, payload } = props;
                  const color = payload.isBullish ? 'green' : 'red';
                  const barHeight = Math.max(payload.bodyTop - payload.bodyBottom, 0.5);
                  return (
                    <rect
                      x={x}
                      y={y}
                      width={width}
                      height={barHeight}
                      fill={color}
                      stroke={color}
                    />
                  );
                }}
              />

              <Legend
                verticalAlign="bottom"
                content={() => (
                  <div style={{ textAlign: 'center', marginTop: '8px' }}>
                    <span style={{ color: 'green', marginRight: '20px' }}>● Bullish Candles</span>
                    <span style={{ color: 'red' }}>● Bearish Candles</span>
                  </div>
                )}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </Box>
      )}
      
      {/* Display color changes as a list */}
      {!isLoading && !error && colorChanges.length > 0 && (
        <Box sx={{ mt: 2 }}>
          <Typography variant="subtitle2" gutterBottom>
            Recent Trend Changes:
          </Typography>
          <ul>
            {colorChanges.slice(0, 5).map((change, index) => (
              <li key={index}>
                <Typography 
                  variant="body2" 
                  color={change.type === 'bullish' ? 'success.main' : 'error.main'}
                >
                  {change.formattedDate}: {change.type.charAt(0).toUpperCase() + change.type.slice(1)} reversal
                </Typography>
              </li>
            ))}
          </ul>
        </Box>
      )}
    </Box>
  );
};

export default HeikinAshiGraph;