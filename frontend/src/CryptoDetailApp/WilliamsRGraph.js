import React from 'react';
import {
  ResponsiveContainer,
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
} from 'recharts';
import { format, parseISO } from 'date-fns';
import { Box, Paper, Typography } from '@mui/material';

const WilliamsRGraph = ({ data, base = 'USD', formatValue }) => {
  if (!data || data.length === 0) {
    return (
      <Box sx={{ p: 2, textAlign: 'center' }}>
        <Typography color="text.secondary">No Williams %R data available</Typography>
      </Box>
    );
  }

  const formatDate = (dateStr) => {
    return format(parseISO(dateStr), 'MMM d, yyyy');
  };

  const formatTooltipValue = (value) => {
    if (typeof value !== 'number') return 'N/A';
    return value.toFixed(2);
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
              {entry.name}: {formatTooltipValue(entry.value)}
            </Typography>
          ))}
        </Paper>
      );
    }
    return null;
  };

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
            domain={[-100, 0]} 
            ticks={[-100, -80, -60, -50, -40, -20, 0]}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend />
          {/* Add reference line for overbought/oversold threshold */}
          <ReferenceLine 
            y={-50} 
            stroke="#666" 
            strokeDasharray="3 3" 
            label={{ 
              value: "Oversold Threshold (-50)", 
              position: "right",
              fill: "#666",
              fontSize: 12
            }} 
          />
          <Line
            type="linear"
            dataKey="williams_r"
            stroke="#ff7300"
            name="Williams %R"
            dot={false}
            strokeWidth={2}
          />
          <Line
            type="linear"
            dataKey="williams_r_ema"
            stroke="#2196f3"
            name="Williams %R EMA"
            dot={false}
            strokeWidth={2}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </Box>
  );
};

export default WilliamsRGraph;