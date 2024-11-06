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
} from 'recharts';
import { format, parseISO } from 'date-fns';
import { Box, Paper, Typography } from '@mui/material';

const ForceIndexGraph = ({ data, base = 'USD', formatValue }) => {
  if (!data || data.length === 0) {
    return (
      <Box sx={{ p: 2, textAlign: 'center' }}>
        <Typography color="text.secondary">No force index data available</Typography>
      </Box>
    );
  }

  const formatDate = (dateStr) => {
    return format(parseISO(dateStr), 'MMM d, yyyy');
  };

  const formatTooltipValue = (value) => {
    return formatValue ? formatValue(value, base) : value.toFixed(base === 'USD' ? 2 : 8);
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
          <YAxis />
          <Tooltip content={<CustomTooltip />} />
          <Legend />
          <Line
            type="linear"
            dataKey="force_index_7_week"
            stroke="#8884d8"
            name={`7-Week Force Index (${base})`}
            dot={false}
            strokeWidth={2}
          />
          <Line
            type="linear"
            dataKey="force_index_52_week"
            stroke="#82ca9d"
            name={`52-Week Force Index (${base})`}
            dot={false}
            strokeWidth={2}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </Box>
  );
};

export default ForceIndexGraph;