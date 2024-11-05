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

const ForceIndexGraph = ({ data }) => {
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

  // Format large numbers in a readable way
  const formatYAxis = (value) => {
    if (Math.abs(value) >= 1e9) {
      return (value / 1e9).toFixed(1) + 'B';
    } else if (Math.abs(value) >= 1e6) {
      return (value / 1e6).toFixed(1) + 'M';
    } else if (Math.abs(value) >= 1e3) {
      return (value / 1e3).toFixed(1) + 'K';
    }
    return value.toFixed(1);
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
              {entry.name}: {formatYAxis(entry.value)}
            </Typography>
          ))}
        </Paper>
      );
    }
    return null;
  };

  // Calculate padding for Y axis based on data range
  const calculateYAxisDomain = () => {
    const values = data.flatMap(d => [
      d.force_index_7_week,
      d.force_index_52_week
    ]).filter(v => v !== null && v !== undefined);
    
    const min = Math.min(...values);
    const max = Math.max(...values);
    const padding = (max - min) * 0.1; // Add 10% padding

    return [min - padding, max + padding];
  };

  return (
    <Box sx={{ width: '100%', height: 400 }}>
      <ResponsiveContainer>
        <ComposedChart
          data={data}
          margin={{ top: 10, right: 30, left: 50, bottom: 70 }} // Increased left margin
        >
          <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
          <XAxis
            dataKey="datetime"
            tickFormatter={formatDate}
            angle={-45}
            textAnchor="end"
            height={60}
            interval={0}
            tick={{ fontSize: 12 }}
          />
          <YAxis 
            tickFormatter={formatYAxis}
            domain={calculateYAxisDomain()}
            width={60} // Ensure enough width for labels
            tick={{ fontSize: 12 }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend 
            verticalAlign="bottom" 
            height={36}
            wrapperStyle={{ 
              bottom: -60,
              fontSize: '12px'
            }}
          />
          <Line
            type="linear"
            dataKey="force_index_7_week"
            stroke="#8884d8"
            name="7-Week Force Index"
            dot={false}
            strokeWidth={2}
          />
          <Line
            type="linear"
            dataKey="force_index_52_week"
            stroke="#82ca9d"
            name="52-Week Force Index"
            dot={false}
            strokeWidth={2}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </Box>
  );
};

export default ForceIndexGraph;