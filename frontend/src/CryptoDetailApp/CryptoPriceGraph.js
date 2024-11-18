import React from 'react';
import {
  ResponsiveContainer,
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  Brush
} from 'recharts';
import { format, parseISO } from 'date-fns';
import { Box, Paper, Typography } from '@mui/material';

const CryptoPriceGraph = ({ data, base = 'USD' }) => {
  if (!data || data.length === 0) {
    return (
      <Box sx={{ p: 2, textAlign: 'center' }}>
        <Typography color="text.secondary">No price data available</Typography>
      </Box>
    );
  }

  const formatDate = (dateStr) => {
    return format(parseISO(dateStr), 'MMM d, yyyy');
  };

  const formatPrice = (value) => {
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

  const formatVolume = (value) => {
    if (!value) return 'N/A';
    return new Intl.NumberFormat('en-US', {
      notation: 'compact',
      compactDisplay: 'short'
    }).format(value);
  };

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <Paper sx={{ p: 2 }}>
          <Typography variant="body2" color="text.secondary">
            {formatDate(label)}
          </Typography>
          {payload.map((entry, index) => (
            <Typography
              key={index}
              variant="body2"
              sx={{ color: entry.color }}
            >
              {entry.name}: {entry.dataKey === 'volume' ? formatVolume(entry.value) : formatPrice(entry.value)}
            </Typography>
          ))}
        </Paper>
      );
    }
    return null;
  };

  // Calculate the domain for the price axis
  const prices = data.map(d => d.close);
  const minPrice = Math.min(...prices) * 0.95; // Add 5% padding
  const maxPrice = Math.max(...prices) * 1.05;

  // Calculate the domain for the volume axis
  const volumes = data.map(d => d.volume);
  const maxVolume = Math.max(...volumes);

  return (
    <Box sx={{ width: '100%', height: 400 }}>
      <ResponsiveContainer>
        <ComposedChart
          data={data}
          margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
        >
          <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
          <XAxis
            dataKey="datetime"
            tickFormatter={formatDate}
            angle={-45}
            textAnchor="end"
            height={60}
          />
          <YAxis
            yAxisId="price"
            domain={[minPrice, maxPrice]}
            tickFormatter={formatPrice}
            orientation="left"
          />
          <YAxis
            yAxisId="volume"
            domain={[0, maxVolume]}
            tickFormatter={formatVolume}
            orientation="right"
            axisLine={false}
            tickLine={false}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend />
          <Line
            yAxisId="price"
            type="monotone"
            dataKey="close"
            stroke="#2196f3"
            name="Price"
            dot={false}
            strokeWidth={2}
          />
          <Line
            yAxisId="price"
            type="monotone"
            dataKey="ema"
            stroke="#4caf50"
            name="200 EMA"
            dot={false}
            strokeWidth={2}
          />
          <Bar
            yAxisId="volume"
            dataKey="volume"
            fill="#90caf9"
            name="Volume"
            opacity={0.5}
          />
          <Brush
            dataKey="datetime"
            height={30}
            stroke="#8884d8"
            tickFormatter={formatDate}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </Box>
  );
};

export default CryptoPriceGraph;