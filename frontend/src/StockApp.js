import React, { useState, useEffect, useCallback } from 'react';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { CircularProgress } from '@mui/material';
import { debounce } from 'lodash';
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

const API_URL = process.env.REACT_APP_API_URL || '/api';

const columnMap = {
  'stock': 'Stock',
  'stock_name': 'Stock Name',
  'market_cap': 'Market Cap',
  'close': 'Last Day Closing Price',
  'pe_ratio': 'P/E Ratio',
  'ev_ebitda': 'EV/EBITDA',
  'pb_ratio': 'P/B Ratio',
  'peg_ratio': 'PEG Ratio',
  'earnings_yield': 'Earnings Yield',
  'book_to_price': 'B/P Ratio',
  'return_on_equity': 'Return on Equity',
  'return_on_assets': 'Return on Assets',
  'price_to_sales': 'Price to Sales',
  'free_cash_flow_yield': 'FCF Yield',
  'shareholder_yield': 'Shareholder Yield',
  'last_quarter_sales': 'Last Quarter Sales',
  'current_quarter_sales': 'Current Quarter Sales',
  'sales_change_percent': 'Sales % Change',
  'last_quarter_ebitda': 'Last Quarter EBITDA',
  'current_quarter_ebitda': 'Current Quarter EBITDA',
  'ebitda_change_percent': 'EBITDA % Change',
  'free_cash_flow': 'Levered Free Cash Flow',
  'ema': '200-EMA',
  'williams_r': 'Williams %R',
  'williams_r_ema': 'Williams %R EMA',
  'williams_r_momentum_alert_state': 'Williams %R Momentum Alert',
  'force_index_7_week': '7-Week Force Index',
  'force_index_52_week': '52-Week Force Index',
  'force_index_alert_state': 'Force Index Alert',
  'anchored_obv_alert_state': 'Anchored OBV Alert',
  'price_change_3m': '3-Month Price Change',
  'price_change_6m': '6-Month Price Change',
  'price_change_12m': '12-Month Price Change',
  'pe_ratio_rank': 'P/E Ratio Ranking',
  'ev_ebitda_rank': 'EV/EBITDA Ranking',
  'pb_ratio_rank': 'P/B Ratio Ranking',
  'peg_ratio_rank': 'PEG Ratio Ranking',
  'earnings_yield_rank': 'Earnings Yield Rank',
  'book_to_price_rank': 'Book to Price Rank',
  'datetime': 'Time'
};

const numericalColumns = [
  'market_cap', 'close', 'pe_ratio', 'ev_ebitda', 'pb_ratio', 
  'peg_ratio', 'current_quarter_sales', 'last_quarter_sales', 'current_quarter_ebitda',
  'last_quarter_ebitda', 'ema', 'williams_r', 'williams_r_ema', 'force_index_7_week',
  'force_index_52_week', 'pe_ratio_rank', 'ev_ebitda_rank', 'pb_ratio_rank',
  'peg_ratio_rank', 'price_change_3m', 'price_change_6m', 'price_change_12m',
  'earnings_yield', 'book_to_price', 'earnings_yield_rank', 'book_to_price_rank', 
  'return_on_equity', 'return_on_assets', 'price_to_sales',
  'free_cash_flow_yield', 'shareholder_yield'
];

const filterColumns = [
  'stock',
  'stock_name',
  'market_cap',
  'pe_ratio',
  'ev_ebitda',
  'pb_ratio',
  'peg_ratio',
  'current_quarter_sales',
  'current_quarter_ebitda',
  'ema',
  'pe_ratio_rank',
  'ev_ebitda_rank',
  'pb_ratio_rank',
  'peg_ratio_rank',
  'earnings_yield_rank',
  'book_to_price_rank',
  'return_on_equity', 
  'return_on_assets', 
  'price_to_sales',
  'free_cash_flow_yield', 
  'shareholder_yield'
];

const alertStateOptions = ['$', '$$$', '-'];
const drawerWidth = 300;

const formatCurrency = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2
  }).format(value);
};

const formatRatio = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return Number(value).toFixed(2);
};

const formatRank = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return Number(value).toFixed(0);
};

const formatPercentage = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return `${(Number(value) * 100).toFixed(2)}%`;
};

const formatColumnValue = (column, value) => {
  switch (column) {
    case 'market_cap':
    case 'current_quarter_sales':
    case 'current_quarter_ebitda':
    case 'last_quarter_sales':
    case 'last_quarter_ebitda':
    case 'free_cash_flow':
    case 'close':
      return formatCurrency(value);
    case 'pe_ratio':
    case 'ev_ebitda':
    case 'pb_ratio':
    case 'peg_ratio':
    case 'earnings_yield':
    case 'book_to_price':      
    case 'ema':
    case 'williams_r':
    case 'williams_r_ema':
    case 'force_index_7_week':
    case 'force_index_52_week':
    case 'return_on_equity':
    case 'return_on_assets':
    case 'free_cash_flow_yield':
    case 'shareholder_yield':  
      return formatRatio(value);
    case 'sales_change_percent':
    case 'ebitda_change_percent':
    case 'price_change_3m':
    case 'price_change_6m':
    case 'price_change_12m':
      return formatPercentage(value);
    case 'pe_ratio_rank':
    case 'ev_ebitda_rank':
    case 'pb_ratio_rank':
    case 'peg_ratio_rank':
    case 'earnings_yield_rank':
    case 'book_to_price_rank':    
      return formatRank(value);
    case 'datetime':
      return new Date(value).toLocaleString();
    default:
      return value;
  }
};

function StockApp({ drawerOpen, toggleDrawer }) {
  const [stockData, setStockData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);
  const [totalPages, setTotalPages] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [filters, setFilters] = useState({});
  const [selectedDate, setSelectedDate] = useState(
    new Date(new Date().setDate(new Date().getDate() - 1))
  );
  const [alertStateFilters, setAlertStateFilters] = useState({
    williams_r_momentum_alert_state: [],
    force_index_alert_state: [],
    anchored_obv_alert_state: []
  });
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'ascending' });
  const [hiddenColumns, setHiddenColumns] = useState([]);
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  const fetchData = useCallback(async () => {
    try {
      setIsLoading(true);
      const formattedDate = selectedDate.toISOString().split('T')[0];
      
      const params = new URLSearchParams({
        date: formattedDate,
        page: page.toString(),
        pageSize: pageSize.toString(),
        sortColumn: sortConfig.key || 'stock',
        sortDirection: sortConfig.direction === 'ascending' ? 'ASC' : 'DESC'
      });

      // Add all filters
      Object.entries(filters).forEach(([key, value]) => {
        if (typeof value === 'string') {
          params.append(key, value.toLowerCase());
        } else if (value && typeof value === 'object') {
          if (value.min !== undefined) params.append(`min_${key}`, value.min);
          if (value.max !== undefined) params.append(`max_${key}`, value.max);
        }
      });

      // Add alert state filters
      Object.entries(alertStateFilters).forEach(([key, values]) => {
        values.forEach(value => {
          params.append(`${key}[]`, value);
        });
      });

      const result = await axios.get(`${API_URL}/stocks/latest?${params}`);
      
      setStockData(result.data.data);
      setTotalPages(result.data.totalPages);
      setTotalCount(result.data.totalCount);
    } catch (error) {
      console.error("Error fetching stock data:", error);
    } finally {
      setIsLoading(false);
    }
  }, [selectedDate, page, pageSize, sortConfig, filters, alertStateFilters]);

  const debouncedFetchData = useCallback(
    debounce(() => fetchData(), 300),
    [fetchData]
  );

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleFilterChange = (column, value, type = null) => {
    setPage(1);
    if (type === null) {
      setFilters(prevFilters => ({
        ...prevFilters,
        [column]: value
      }));
    } else {
      setFilters(prevFilters => ({
        ...prevFilters,
        [column]: { ...(prevFilters[column] || {}), [type]: value }
      }));
    }
    debouncedFetchData();
  };

  const handleAlertStateFilterChange = (column, value) => {
    setPage(1);
    setAlertStateFilters(prevFilters => ({
      ...prevFilters,
      [column]: prevFilters[column].includes(value)
        ? prevFilters[column].filter(v => v !== value)
        : [...prevFilters[column], value]
    }));
    debouncedFetchData();
  };

  const clearFilters = () => {
    setFilters({});
    setAlertStateFilters({
      williams_r_momentum_alert_state: [],
      force_index_alert_state: [],
      anchored_obv_alert_state: []
    });
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

  const visibleColumns = Object.keys(columnMap).filter(
    column => !hiddenColumns.includes(column)
  );

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
          label="Select Date"
          value={selectedDate}
          onChange={setSelectedDate}
          maxDate={new Date(new Date().setDate(new Date().getDate() - 1))}
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

        {filterColumns.map((column) => (
          <ListItem key={column} sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
            <Typography variant="body2" sx={{ mb: 1, fontWeight: 'bold' }}>
              {columnMap[column]}
            </Typography>
            {column === 'stock' || column === 'stock_name' ? (
              <TextField
                fullWidth
                size="small"
                placeholder={`Filter ${columnMap[column]}`}
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

        {Object.entries(alertStateFilters).map(([column, selectedValues]) => (
          <ListItem key={column} sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
            <Typography variant="body2" sx={{ mb: 1, fontWeight: 'bold' }}>
              {columnMap[column]}
            </Typography>
            <FormGroup>
              {alertStateOptions.map((option) => (
                <FormControlLabel
                  key={option}
                  control={
                    <Checkbox
                      checked={selectedValues.includes(option)}
                      onChange={() => handleAlertStateFilterChange(column, option)}
                    />
                  }
                  label={option}
                />
              ))}
            </FormGroup>
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
                      align={key === 'stock' || key === 'stock_name' ? "left" : "center"}
                      sx={{ 
                        whiteSpace: 'nowrap', 
                        padding: '8px 12px',
                        fontSize: '0.9rem',
                        fontWeight: 'bold',
                        backgroundColor: '#f8f9fa',
                        ...(key === 'stock_name' && { width: '200px' })
                      }}
                    >
                      <Box sx={{ 
                        display: 'flex', 
                        alignItems: 'center', 
                        justifyContent: key === 'stock' || key === 'stock_name' ? "flex-start" : "center" 
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
                {stockData.map((stock, index) => (
                  <TableRow 
                    key={`${stock.stock}-${index}`} 
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
                        align={column === 'stock' || column === 'stock_name' ? "left" : "center"}
                        sx={{ 
                          whiteSpace: 'nowrap', 
                          padding: '8px 12px',
                          fontSize: '0.85rem',
                          ...(column === 'stock_name' && {
                            maxWidth: '200px',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis'
                          })
                        }}
                      >
                        {column === 'stock' ? (
                          <Link 
                            to={`/stock/${stock[column]}`}
                            style={{ 
                              color: '#1976d2', 
                              textDecoration: 'none',
                              '&:hover': {
                                textDecoration: 'underline'
                              }
                            }}
                          >
                            {formatColumnValue(column, stock[column])}
                          </Link>
                        ) : column === 'stock_name' ? (
                          <Tooltip title={stock[column]} placement="top">
                            <span>{formatColumnValue(column, stock[column])}</span>
                          </Tooltip>
                        ) : ['price_change_3m', 'price_change_6m', 'price_change_12m'].includes(column) ? (
                          <span style={{ 
                            color: stock[column] > 0 ? '#4caf50' : stock[column] < 0 ? '#f44336' : 'inherit'
                          }}>
                            {formatColumnValue(column, stock[column])}
                          </span>
                        ) : (
                          formatColumnValue(column, stock[column])
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

export default StockApp;