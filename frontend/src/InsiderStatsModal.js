import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  IconButton,
  Typography,
  Box,
  CircularProgress,
  Grid,
  Card,
  CardContent,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';

const StatCard = ({ title, value, subtitle }) => (
  <Card sx={{ height: '100%' }}>
    <CardContent>
      <Typography color="textSecondary" gutterBottom>
        {title}
      </Typography>
      <Typography variant="h4" component="div">
        {value}
      </Typography>
      {subtitle && (
        <Typography color="textSecondary" sx={{ fontSize: 14 }}>
          {subtitle}
        </Typography>
      )}
    </CardContent>
  </Card>
);

const formatPercentage = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return `${(value * 100).toFixed(2)}%`;
};

const formatWinRate = (wins, total) => {
  if (!total) return 'N/A';
  return `${wins}/${total} (${((wins/total) * 100).toFixed(2)}%)`;
};

const InsiderStatsModal = ({ open, onClose, insiderName, stats, isLoading, error }) => {
  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
    >
      <DialogTitle>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Typography variant="h6">
            Trading Statistics: {insiderName}
          </Typography>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>
      <DialogContent>
        {isLoading ? (
          <Box display="flex" justifyContent="center" alignItems="center" minHeight={300}>
            <CircularProgress />
          </Box>
        ) : error ? (
          <Box display="flex" justifyContent="center" alignItems="center" minHeight={300}>
            <Typography color="error">{error}</Typography>
          </Box>
        ) : (
          <Grid container spacing={3} sx={{ mt: 1 }}>
            <Grid item xs={12} sm={6} md={4}>
              <StatCard
                title="Total Purchases"
                value={stats?.totalPurchases || 0}
                subtitle="All-time purchase transactions"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <StatCard
                title="1-Month Win Rate"
                value={formatWinRate(stats?.oneMonthWins, stats?.totalPurchases)}
                subtitle="Positive returns after 1 month"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <StatCard
                title="1-Month Average Return"
                value={formatPercentage(stats?.avgOneMonthReturn)}
                subtitle="Average return after 1 month"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <StatCard
                title="3-Month Win Rate"
                value={formatWinRate(stats?.threeMonthWins, stats?.totalPurchases)}
                subtitle="Positive returns after 3 months"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <StatCard
                title="3-Month Average Return"
                value={formatPercentage(stats?.avgThreeMonthReturn)}
                subtitle="Average return after 3 months"
              />
            </Grid>
          </Grid>
        )}
      </DialogContent>
    </Dialog>
  );
};

export default InsiderStatsModal;