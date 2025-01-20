import React, { useState } from 'react';
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
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  TableSortLabel,
  Paper
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';

// Utility Components
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

// Formatting Functions
const formatCurrency = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return new Intl.NumberFormat('en-US', { 
    style: 'currency', 
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2 
  }).format(value);
};

const formatNumber = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return new Intl.NumberFormat('en-US').format(value);
};

const formatPercentage = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return `${(value * 100).toFixed(2)}%`;
};

const formatWinRate = (wins, total) => {
  if (!total) return 'N/A';
  return `${wins}/${total} (${((wins/total) * 100).toFixed(2)}%)`;
};

const formatDate = (dateString) => {
  return new Date(dateString).toLocaleDateString();
};

// Main Component
const IN_InsiderStatsModal = ({ open, onClose, insiderName, stats, isLoading, error }) => {
  // State for pagination and sorting
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [orderBy, setOrderBy] = useState('datetime');
  const [order, setOrder] = useState('desc');

  // Handlers
  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleSort = (property) => () => {
    const isAsc = orderBy === property && order === 'asc';
    setOrder(isAsc ? 'desc' : 'asc');
    setOrderBy(property);
    setPage(0); // Reset to first page when sorting changes
  };

  // Sorting functions
  const descendingComparator = (a, b, orderBy) => {
    // Handle null values
    if (!a[orderBy] && !b[orderBy]) return 0;
    if (!a[orderBy]) return 1;
    if (!b[orderBy]) return -1;

    // Handle dates
    if (orderBy === 'datetime') {
      return new Date(b.datetime).getTime() - new Date(a.datetime).getTime();
    }

    // Handle numeric values
    if (typeof a[orderBy] === 'number' && typeof b[orderBy] === 'number') {
      return b[orderBy] - a[orderBy];
    }

    // Handle strings
    return b[orderBy].toString().localeCompare(a[orderBy].toString());
  };

  const getComparator = (order, orderBy) => {
    return order === 'desc'
      ? (a, b) => descendingComparator(a, b, orderBy)
      : (a, b) => -descendingComparator(a, b, orderBy);
  };

  // Sort and paginate data
  const sortAndPaginateData = (data) => {
    if (!data) return [];
    
    const stabilizedThis = data.map((el, index) => [el, index]);
    
    stabilizedThis.sort((a, b) => {
      const orderValue = getComparator(order, orderBy)(a[0], b[0]);
      if (orderValue !== 0) return orderValue;
      return a[1] - b[1]; // Maintain relative order of equal items
    });

    const sortedData = stabilizedThis.map((el) => el[0]);

    return sortedData.slice(
      page * rowsPerPage,
      page * rowsPerPage + rowsPerPage
    );
  };

  const displayedRows = sortAndPaginateData(stats?.transactions || []);
  const totalRows = stats?.transactions?.length || 0;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="lg"
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
          <>
            {/* Stats Cards */}
            <Grid container spacing={3} sx={{ mt: 1, mb: 4 }}>
              <Grid item xs={12} sm={6} md={4}>
                <StatCard
                  title="Total Purchases"
                  value={stats?.stats?.totalPurchases || 0}
                  subtitle="All-time purchase transactions"
                />
              </Grid>
              <Grid item xs={12} sm={6} md={4}>
                <StatCard
                  title="1-Month Win Rate"
                  value={formatWinRate(stats?.stats?.oneMonthWins, stats?.stats?.totalPurchases)}
                  subtitle="Positive returns after 1 month"
                />
              </Grid>
              <Grid item xs={12} sm={6} md={4}>
                <StatCard
                  title="1-Month Average Return"
                  value={formatPercentage(stats?.stats?.avgOneMonthReturn)}
                  subtitle="Average return after 1 month"
                />
              </Grid>
              <Grid item xs={12} sm={6} md={4}>
                <StatCard
                  title="3-Month Win Rate"
                  value={formatWinRate(stats?.stats?.threeMonthWins, stats?.stats?.totalPurchases)}
                  subtitle="Positive returns after 3 months"
                />
              </Grid>
              <Grid item xs={12} sm={6} md={4}>
                <StatCard
                  title="3-Month Average Return"
                  value={formatPercentage(stats?.stats?.avgThreeMonthReturn)}
                  subtitle="Average return after 3 months"
                />
              </Grid>
            </Grid>

            {/* Transactions Table */}
            <Box sx={{ mb: 2 }}>
              <Typography variant="h6" gutterBottom>
                Purchase History
              </Typography>
            </Box>
            <TableContainer component={Paper}>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>
                      <TableSortLabel
                        active={orderBy === 'datetime'}
                        direction={orderBy === 'datetime' ? order : 'asc'}
                        onClick={handleSort('datetime')}
                      >
                        Date
                      </TableSortLabel>
                    </TableCell>
                    <TableCell>
                      <TableSortLabel
                        active={orderBy === 'stock'}
                        direction={orderBy === 'stock' ? order : 'asc'}
                        onClick={handleSort('stock')}
                      >
                        Stock
                      </TableSortLabel>
                    </TableCell>
                    <TableCell align="right">
                      <TableSortLabel
                        active={orderBy === 'shares_traded'}
                        direction={orderBy === 'shares_traded' ? order : 'asc'}
                        onClick={handleSort('shares_traded')}
                      >
                        Shares
                      </TableSortLabel>
                    </TableCell>
                    <TableCell align="right">
                      <TableSortLabel
                        active={orderBy === 'price_per_share'}
                        direction={orderBy === 'price_per_share' ? order : 'asc'}
                        onClick={handleSort('price_per_share')}
                      >
                        Price/Share
                      </TableSortLabel>
                    </TableCell>
                    <TableCell align="right">
                      <TableSortLabel
                        active={orderBy === 'total_value'}
                        direction={orderBy === 'total_value' ? order : 'asc'}
                        onClick={handleSort('total_value')}
                      >
                        Total Value
                      </TableSortLabel>
                    </TableCell>
                    <TableCell align="right">
                      <TableSortLabel
                        active={orderBy === 'one_month_return'}
                        direction={orderBy === 'one_month_return' ? order : 'asc'}
                        onClick={handleSort('one_month_return')}
                      >
                        1M Return
                      </TableSortLabel>
                    </TableCell>
                    <TableCell align="right">
                      <TableSortLabel
                        active={orderBy === 'three_month_return'}
                        direction={orderBy === 'three_month_return' ? order : 'asc'}
                        onClick={handleSort('three_month_return')}
                      >
                        3M Return
                      </TableSortLabel>
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {displayedRows.map((transaction) => (
                    <TableRow key={`${transaction.datetime}-${transaction.stock}-${transaction.shares_traded}`}>
                      <TableCell>{formatDate(transaction.datetime)}</TableCell>
                      <TableCell>{transaction.stock}</TableCell>
                      <TableCell align="right">{formatNumber(transaction.shares_traded)}</TableCell>
                      <TableCell align="right">{formatCurrency(transaction.price_per_share)}</TableCell>
                      <TableCell align="right">{formatCurrency(transaction.total_value)}</TableCell>
                      <TableCell 
                        align="right"
                        sx={{ 
                          color: transaction.one_month_return > 0 ? 'success.main' : 
                                transaction.one_month_return < 0 ? 'error.main' : 
                                'text.primary'
                        }}
                      >
                        {formatPercentage(transaction.one_month_return)}
                      </TableCell>
                      <TableCell 
                        align="right"
                        sx={{ 
                          color: transaction.three_month_return > 0 ? 'success.main' : 
                                transaction.three_month_return < 0 ? 'error.main' : 
                                'text.primary'
                        }}
                      >
                        {formatPercentage(transaction.three_month_return)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              <TablePagination
                rowsPerPageOptions={[5, 10, 25]}
                component="div"
                count={totalRows}
                rowsPerPage={rowsPerPage}
                page={page}
                onPageChange={handleChangePage}
                onRowsPerPageChange={handleChangeRowsPerPage}
              />
            </TableContainer>
          </>
        )}
      </DialogContent>
    </Dialog>
  );
};

export default IN_InsiderStatsModal;