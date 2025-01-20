import React, { useState, useEffect, useCallback } from 'react';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { CircularProgress } from '@mui/material';
import axios from 'axios';
import { 
  Table, TableBody, TableCell, TableHead, TableRow, Paper,
  TextField, Button, Typography, Box, Drawer, List, ListItem,
  Divider, useMediaQuery, useTheme, Grid, Checkbox, FormGroup, FormControlLabel,
  Tooltip, Select, MenuItem, OutlinedInput, TableContainer, Pagination,
  FormControl, InputLabel
} from '@mui/material';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import { Link } from 'react-router-dom';
import { debounce } from 'lodash';
import US_InsiderStatsModal from './US_InsiderStatsModal';

const API_URL = process.env.REACT_APP_API_URL || '/api';

const columnMap = {
  'stock': 'Stock',
  'stock_name': 'Company Name',
  'insider_name': 'Insider Name',
  'transaction_type': 'Transaction Type',
  'relationship': 'Relationship',
  'shares_traded': 'Shares Traded',
  'price_per_share': 'Price/Share',
  'total_value': 'Total Value',
  'shares_owned_following': 'Shares Owned After',
  'one_month_price': '1M Price',
  'three_month_price': '3M Price',
  'one_month_return': '1M Return',
  'three_month_return': '3M Return',
  'datetime': 'Transaction Date'
};

const numericalColumns = [
  'shares_traded',
  'price_per_share',
  'total_value',
  'shares_owned_following',
  'one_month_price',
  'three_month_price',
  'one_month_return',
  'three_month_return'
];

const filterColumns = [
  'stock',
  'insider_name',
  'shares_traded',
  'total_value',
  'one_month_return',
  'three_month_return'
];

const transactionTypes = [
  { value: 'P', label: 'Purchase' },
  { value: 'S', label: 'Sale' }
];

const drawerWidth = 300;

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

const getRelationshipString = (trade) => {
  const relationships = [];
  if (trade.relationship_is_director) relationships.push('Director');
  if (trade.relationship_is_officer) relationships.push('Officer');
  if (trade.relationship_is_ten_percent_owner) relationships.push('10% Owner');
  if (trade.relationship_is_other) relationships.push('Other');
  return relationships.join(', ');
};

const formatTransactionType = (type) => {
  switch (type) {
    case 'P':
      return 'Purchase';
    case 'S':
      return 'Sale';
    default:
      return type;
  }
};

const formatColumnValue = (column, value, rowData) => {
  switch (column) {
    case 'price_per_share':
    case 'one_month_price':
    case 'three_month_price':
    case 'total_value':
      return formatCurrency(value);
    case 'shares_traded':
    case 'shares_owned_following':
      return formatNumber(value);
    case 'one_month_return':
    case 'three_month_return':
      return formatPercentage(value);
    case 'relationship':
      return getRelationshipString(rowData);
    case 'transaction_type':
      return formatTransactionType(value);
    case 'datetime':
      return new Date(value).toLocaleDateString();
    default:
      return value;
  }
};

function US_InsiderTradingApp({ drawerOpen, toggleDrawer }) {
  const [insiderData, setInsiderData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);
  const [totalPages, setTotalPages] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [filters, setFilters] = useState({});
  const [selectedTransactionTypes, setSelectedTransactionTypes] = useState([]);
  const [startDate, setStartDate] = useState(new Date(new Date().setDate(new Date().getDate() - 30)));
  const [endDate, setEndDate] = useState(new Date());
  const [sortConfig, setSortConfig] = useState({ key: 'datetime', direction: 'descending' });
  const [hiddenColumns, setHiddenColumns] = useState([]);
  
  const [selectedInsider, setSelectedInsider] = useState(null);
  const [insiderStats, setInsiderStats] = useState(null);
  const [isStatsLoading, setIsStatsLoading] = useState(false);
  const [statsError, setStatsError] = useState(null);

  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  // Fetch insider statistics
  const fetchInsiderStats = async (insiderName) => {
    setIsStatsLoading(true);
    setStatsError(null);
    try {
      const response = await axios.get(`${API_URL}/stocks/insider/stats/${encodeURIComponent(insiderName)}`);
      setInsiderStats(response.data);
    } catch (error) {
      setStatsError(error.response?.data?.error || 'Failed to fetch insider statistics');
      console.error('Error fetching insider stats:', error);
    } finally {
      setIsStatsLoading(false);
    }
  };

  // Handle insider name click
  const handleInsiderClick = (insiderName) => {
    setSelectedInsider(insiderName);
    fetchInsiderStats(insiderName);
  };

  // Handle modal close
  const handleModalClose = () => {
    setSelectedInsider(null);
    setInsiderStats(null);
    setStatsError(null);
  };

  const fetchData = useCallback(async () => {
    try {
      setIsLoading(true);
      const startDateStr = startDate.toISOString().split('T')[0];
      const endDateStr = endDate.toISOString().split('T')[0];
      
      const params = new URLSearchParams({
        page: page.toString(),
        pageSize: pageSize.toString(),
        start_date: startDateStr,
        end_date: endDateStr,
        sortColumn: sortConfig.key || 'datetime',
        sortDirection: sortConfig.direction === 'ascending' ? 'ASC' : 'DESC'
      });

      Object.entries(filters).forEach(([key, value]) => {
        if (key === 'insider_name' || key === 'stock') {
          if (value) params.append(key, value);
        } else {
          if (value.min) params.append(`min_${key}`, value.min);
          if (value.max) params.append(`max_${key}`, value.max);
        }
      });

      if (selectedTransactionTypes.length > 0) {
        selectedTransactionTypes.forEach(type => {
          params.append('transaction_type[]', type);
        });
      }

      const result = await axios.get(`${API_URL}/stocks/insider?${params}`);
      setInsiderData(result.data.data);
      setTotalPages(result.data.totalPages);
      setTotalCount(result.data.totalCount);
    } catch (error) {
      console.error("Error fetching insider trading data:", error);
    } finally {
      setIsLoading(false);
    }
  }, [page, pageSize, startDate, endDate, sortConfig, filters, selectedTransactionTypes]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const debouncedFetchData = useCallback(
    debounce(() => fetchData(), 300),
    [fetchData]
  );

  const handleFilterChange = (column, value, type) => {
    setPage(1);
    if (column === 'insider_name' || column === 'stock') {
      setFilters(prevFilters => ({
        ...prevFilters,
        [column]: value.toLowerCase()
      }));
    } else {
      setFilters(prevFilters => ({
        ...prevFilters,
        [column]: { ...prevFilters[column], [type]: value }
      }));
    }
    debouncedFetchData();
  };

  const handleTransactionTypeChange = (type) => {
    setPage(1);
    setSelectedTransactionTypes(prev => {
      if (prev.includes(type)) {
        return prev.filter(t => t !== type);
      } else {
        return [...prev, type];
      }
    });
    debouncedFetchData();
  };

  const clearFilters = () => {
    setFilters({});
    setSelectedTransactionTypes([]);
    setPage(1);
    fetchData();
  };

  const handlePageChange = (event, newPage) => {
    setPage(newPage);
  };

  const handlePageSizeChange = (event) => {
    setPageSize(event.target.value);
    setPage(1);
  };

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const handleColumnVisibilityChange = (event) => {
    const { value } = event.target;
    setHiddenColumns(typeof value === 'string' ? value.split(',') : value);
  };

  const visibleColumns = Object.keys(columnMap).filter(column => !hiddenColumns.includes(column));

  const drawer = (
    <Box sx={{ p: 2 }}>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mb: 2, mt: 8 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">Filters</Typography>
          <Button onClick={toggleDrawer}>
            <ChevronLeftIcon />
          </Button>
        </Box>
        <DatePicker
          label="Start Date"
          value={startDate}
          onChange={setStartDate}
          maxDate={endDate}
          slotProps={{ textField: { size: "small", fullWidth: true } }}
        />
        <DatePicker
          label="End Date"
          value={endDate}
          onChange={setEndDate}
          minDate={startDate}
          maxDate={new Date()}
          slotProps={{ textField: { size: "small", fullWidth: true } }}
        />
      </Box>
      <Divider />
      <List>
        <ListItem sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
          <Typography variant="body2" sx={{ mb: 1, fontWeight: 'bold' }}>
            Hide Columns
          </Typography>
          <Select
            multiple
            value={hiddenColumns}
            onChange={handleColumnVisibilityChange}
            input={<OutlinedInput />}
            renderValue={(selected) => selected.map(col => columnMap[col]).join(', ')}
            sx={{ width: '100%' }}
          >
            {Object.entries(columnMap).map(([key, value]) => (
              <MenuItem key={key} value={key}>
                <Checkbox checked={hiddenColumns.indexOf(key) > -1} />
                {value}
              </MenuItem>
            ))}
          </Select>
        </ListItem>

        <ListItem sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
          <Typography variant="body2" sx={{ mb: 1, fontWeight: 'bold' }}>
            Transaction Type
          </Typography>
          <FormGroup>
            {transactionTypes.map((type) => (
              <FormControlLabel
                key={type.value}
                control={
                  <Checkbox
                    checked={selectedTransactionTypes.includes(type.value)}
                    onChange={() => handleTransactionTypeChange(type.value)}
                  />
                }
                label={type.label}
              />
            ))}
          </FormGroup>
        </ListItem>

        {filterColumns.map((column) => (
          <ListItem key={column} sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
            <Typography variant="body2" sx={{ mb: 1, fontWeight: 'bold' }}>
              {columnMap[column]}
            </Typography>
            {column === 'insider_name' || column === 'stock' ? (
              <TextField
                fullWidth
                size="small"
                placeholder={`Filter by ${column === 'stock' ? 'symbol' : 'name'}`}
                onChange={(e) => handleFilterChange(column, e.target.value)}
                value={filters[column] || ''}
              />
            ) : (
              <Grid container spacing={1}>
                <Grid item xs={6}>
                  <TextField
                    fullWidth
                    size="small"
                    placeholder="Min"
                    type="number"
                    onChange={(e) => handleFilterChange(column, parseFloat(e.target.value), 'min')}
                  />
                </Grid>
                <Grid item xs={6}>
                  <TextField
                    fullWidth
                    size="small"
                    placeholder="Max"
                    type="number"
                    onChange={(e) => handleFilterChange(column, parseFloat(e.target.value), 'max')}
                  />
                </Grid>
              </Grid>
            )}
          </ListItem>
        ))}

        <ListItem sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
          <FormControl fullWidth size="small">
            <InputLabel>Rows per page</InputLabel>
            <Select
              value={pageSize}
              onChange={handlePageSizeChange}
              label="Rows per page"
            >
              <MenuItem value={50}>50</MenuItem>
              <MenuItem value={100}>100</MenuItem>
              <MenuItem value={250}>250</MenuItem>
              <MenuItem value={500}>500</MenuItem>
            </Select>
          </FormControl>
        </ListItem>
      </List>
      <Box sx={{ mt: 2 }}>
        <Button variant="contained" fullWidth onClick={fetchData} sx={{ mb: 1 }}>
          Apply Filters
        </Button>
        <Button variant="outlined" fullWidth onClick={clearFilters}>
          Clear Filters
        </Button>
      </Box>
    </Box>
  );

  return (
    <Box sx={{ 
      display: 'flex', 
      flexDirection: 'column', 
      height: 'calc(100vh - 64px)',
      overflow: 'hidden',
    }}>
      <Drawer
        variant={isMobile ? "temporary" : "persistent"}
        open={drawerOpen}
        onClose={toggleDrawer}
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          '& .MuiDrawer-paper': {
            width: drawerWidth,
            boxSizing: 'border-box',
          },
        }}
      >
        {drawer}
      </Drawer>

      {/* Insider Stats Modal */}
      <US_InsiderStatsModal
        open={Boolean(selectedInsider)}
        onClose={handleModalClose}
        insiderName={selectedInsider}
        stats={insiderStats}
        isLoading={isStatsLoading}
        error={statsError}
      />

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          display: 'flex',
          flexDirection: 'column',
          width: '100%',
          height: '100%',
          overflow: 'hidden',
          transition: theme.transitions.create(['margin', 'width'], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.leavingScreen,
          }),
          ...(drawerOpen && {
            marginLeft: `${drawerWidth}px`,
            width: `calc(100% - ${drawerWidth}px)`,
          }),
        }}
      >
        <TableContainer component={Paper} sx={{ flexGrow: 1, overflow: 'auto' }}>
          {isLoading ? (
            <Box sx={{ 
              display: 'flex', 
              justifyContent: 'center', 
              alignItems: 'center', 
              height: '100%',
              p: 4
            }}>
              <CircularProgress />
            </Box>
          ) : (
            <Table stickyHeader>
              <TableHead>
                <TableRow>
                  {visibleColumns.map((key) => (
                    <TableCell 
                      key={key}
                      align={key === 'stock' || key === 'stock_name' || key === 'insider_name' ? "left" : "center"}
                      sx={{ 
                        whiteSpace: 'nowrap', 
                        padding: '8px 12px',
                        fontSize: '0.9rem',
                        fontWeight: 'bold',
                        backgroundColor: '#f8f9fa'
                      }}
                    >
                      <Box sx={{ 
                        display: 'flex', 
                        alignItems: 'center', 
                        justifyContent: key === 'stock' || key === 'stock_name' || key === 'insider_name' ? "flex-start" : "center" 
                      }}>
                        {columnMap[key]}
                        {(numericalColumns.includes(key) || key === 'datetime') && (
                          <Button size="small" onClick={() => requestSort(key)}>
                            {sortConfig.key === key ? (
                              sortConfig.direction === 'ascending' ? (
                                <ArrowUpwardIcon fontSize="inherit" />
                              ) : (
                                <ArrowDownwardIcon fontSize="inherit" />
                              )
                            ) : (
                              <ArrowUpwardIcon fontSize="inherit" color="disabled" />
                            )}
                          </Button>
                        )}
                      </Box>
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {insiderData.map((trade, index) => (
                  <TableRow 
                    key={`${trade.datetime}-${trade.stock}-${trade.insider_name}-${index}`} 
                    hover
                    sx={{
                      '&:nth-of-type(odd)': {
                        backgroundColor: 'rgba(0, 0, 0, 0.02)',
                      },
                    }}
                  >
                    {visibleColumns.map((column) => (
                      <TableCell 
                        key={column}
                        align={column === 'stock' || column === 'stock_name' || column === 'insider_name' ? "left" : "center"}
                        sx={{ 
                          whiteSpace: 'nowrap', 
                          padding: '8px 12px',
                          fontSize: '0.85rem',
                          ...(column === 'stock_name' && {
                            maxWidth: '200px',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis'
                          }),
                          ...(column === 'transaction_type' && {
                            color: trade[column] === 'P' ? '#4caf50' : '#f44336'
                          })
                        }}
                      >
                        {column === 'stock' ? (
                          <Link 
                            to={`/us_stock/${trade[column]}`}
                            style={{ 
                              color: '#1976d2', 
                              textDecoration: 'none',
                              '&:hover': {
                                textDecoration: 'underline'
                              }
                            }}
                          >
                            {formatColumnValue(column, trade[column], trade)}
                          </Link>
                        ) : column === 'insider_name' ? (
                          <Button
                            onClick={() => handleInsiderClick(trade[column])}
                            sx={{
                              padding: 0,
                              textTransform: 'none',
                              fontWeight: 'normal',
                              fontSize: '0.85rem',
                              color: 'primary.main',
                              '&:hover': {
                                backgroundColor: 'transparent',
                                textDecoration: 'underline'
                              }
                            }}
                          >
                            {formatColumnValue(column, trade[column], trade)}
                          </Button>
                        ) : column === 'stock_name' ? (
                          <Tooltip title={trade[column]} placement="top">
                            <span>{formatColumnValue(column, trade[column], trade)}</span>
                          </Tooltip>
                        ) : column === 'one_month_return' || column === 'three_month_return' ? (
                          <span style={{ 
                            color: trade[column] > 0 ? '#4caf50' : trade[column] < 0 ? '#f44336' : 'inherit'
                          }}>
                            {formatColumnValue(column, trade[column], trade)}
                          </span>
                        ) : (
                          formatColumnValue(column, trade[column], trade)
                        )}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </TableContainer>

        {/* Pagination Controls */}
        <Box sx={{ 
          p: 2, 
          display: 'flex', 
          justifyContent: 'space-between',
          alignItems: 'center',
          borderTop: 1,
          borderColor: 'divider',
          backgroundColor: 'background.paper'
        }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Typography variant="body2">
              Total records: {totalCount}
            </Typography>
            <Typography variant="body2">
              Page {page} of {totalPages}
            </Typography>
          </Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <FormControl size="small" sx={{ minWidth: 120 }}>
              <Select
                value={pageSize}
                onChange={handlePageSizeChange}
                variant="outlined"
                sx={{ height: 32 }}
              >
                <MenuItem value={50}>50 per page</MenuItem>
                <MenuItem value={100}>100 per page</MenuItem>
                <MenuItem value={250}>250 per page</MenuItem>
                <MenuItem value={500}>500 per page</MenuItem>
              </Select>
            </FormControl>
            <Pagination 
              count={totalPages}
              page={page}
              onChange={handlePageChange}
              color="primary"
              showFirstButton
              showLastButton
              size="small"
              siblingCount={isMobile ? 0 : 1}
              boundaryCount={isMobile ? 1 : 2}
            />
          </Box>
        </Box>
      </Box>
    </Box>
  );
}

export default US_InsiderTradingApp;