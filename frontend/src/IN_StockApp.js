import React, { useState, useEffect, useCallback, useRef } from 'react';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { CircularProgress, IconButton } from '@mui/material';
import axios from 'axios';
import { 
  Table, TableBody, TableCell, TableHead, TableRow, Paper,
  TextField, Button, Typography, Box, Drawer, List, ListItem,
  Divider, useMediaQuery, useTheme, FormGroup, FormControlLabel,
  Tooltip, Select, MenuItem, OutlinedInput, TableContainer, Pagination,
  FormControl, InputLabel, Fab, Checkbox
} from '@mui/material';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import FilterAltIcon from '@mui/icons-material/FilterAlt';
import ClearIcon from '@mui/icons-material/Clear';
import { Link } from 'react-router-dom';

const API_URL = process.env.REACT_APP_API_URL || '/api';
const drawerWidth = 300;

const STORAGE_KEYS = {
  FILTERS: 'stockAppFilters',
  HIDDEN_COLUMNS: 'stockAppHiddenColumns',
  PAGE_SIZE: 'stockAppPageSize'
};

const columnMap = {
  'stock': 'Stock',
  'stock_name': 'Stock Name',
  'market_cap': 'Market Cap',
  'close': 'Closing Price',
  'pe_ratio': 'P/E Ratio',
  'ev_ebitda': 'EV/EBITDA',
  'pb_ratio': 'P/B Ratio',
  'peg_ratio': 'PEG Ratio',
  'earnings_yield': 'Earnings Yield',
  'book_to_price': 'B/P Ratio',
  'return_on_equity': 'ROE',
  'return_on_assets': 'ROA',
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
  'book_to_price_rank': 'B/P Rank',
  'erp5_rank': 'ERP5 Rank',
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
  'free_cash_flow_yield', 'shareholder_yield', 'erp5_rank'
];

const filterColumns = [
  'market_cap',
  'pe_ratio',
  'ev_ebitda',
  'pb_ratio',
  'peg_ratio',
  'current_quarter_sales',
  'current_quarter_ebitda',
  'pe_ratio_rank',
  'ev_ebitda_rank',
  'pb_ratio_rank',
  'peg_ratio_rank',
  'earnings_yield_rank',
  'book_to_price_rank',
  'erp5_rank',
  'return_on_equity', 
  'return_on_assets', 
  'price_to_sales',
  'free_cash_flow_yield', 
  'shareholder_yield'
];

const textFilterColumns = ['stock', 'stock_name'];
const alertStateOptions = ['$', '$$$', '-'];
const initialAlertStates = {
  williams_r_momentum_alert_state: [],
  force_index_alert_state: [],
  anchored_obv_alert_state: []
};

// Utilities for localStorage
const loadSavedFilters = () => {
  try {
    const savedFilters = localStorage.getItem(STORAGE_KEYS.FILTERS);
    return savedFilters ? JSON.parse(savedFilters) : {
      numerical: {},
      text: {},
      alerts: {...initialAlertStates}
    };
  } catch (error) {
    console.error('Error loading saved filters:', error);
    return {
      numerical: {},
      text: {},
      alerts: {...initialAlertStates}
    };
  }
};

const saveFilters = (newFilters) => {
  try {
    localStorage.setItem(STORAGE_KEYS.FILTERS, JSON.stringify(newFilters));
  } catch (error) {
    console.error('Error saving filters:', error);
  }
};

const loadSavedSettings = () => {
  try {
    return {
      hiddenColumns: JSON.parse(localStorage.getItem(STORAGE_KEYS.HIDDEN_COLUMNS)) || [],
      pageSize: parseInt(localStorage.getItem(STORAGE_KEYS.PAGE_SIZE)) || 100
    };
  } catch (error) {
    console.error('Error loading saved settings:', error);
    return {
      hiddenColumns: [],
      pageSize: 100
    };
  }
};

// Formatting functions
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
    case 'erp5_rank':          
      return formatRank(value);
    case 'datetime':
      return new Date(value).toLocaleString();
    default:
      return value;
  }
};

function IN_StockApp({ drawerOpen, toggleDrawer }) {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));
  const savedSettings = loadSavedSettings();

  // State declarations
  const [stockData, setStockData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(savedSettings.pageSize);
  const [totalPages, setTotalPages] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [selectedDate, setSelectedDate] = useState(
    new Date(new Date().setDate(new Date().getDate() - 1))
  );
  const [hiddenColumns, setHiddenColumns] = useState(savedSettings.hiddenColumns);
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'ascending' });
  const [hasFilterChanges, setHasFilterChanges] = useState(false);
  const [filters, setFilters] = useState(loadSavedFilters());
  const [pendingFilters, setPendingFilters] = useState(loadSavedFilters());
  const prevFilters = useRef(filters);
  const prevDate = useRef(selectedDate);

// Utility functions
  const hasActiveFilters = useCallback(() => {
    return (
      Object.keys(filters.numerical).length > 0 ||
      Object.keys(filters.text).length > 0 ||
      Object.values(filters.alerts).some(arr => arr.length > 0)
    );
  }, [filters]);

  // Data fetching
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

      // Add numerical filters
      Object.entries(filters.numerical).forEach(([key, value]) => {
        if (value?.min !== undefined) params.append(`min_${key}`, value.min);
        if (value?.max !== undefined) params.append(`max_${key}`, value.max);
      });

      // Add text filters
      Object.entries(filters.text).forEach(([key, value]) => {
        if (value) params.append(key, value);
      });

      // Add alert state filters
      Object.entries(filters.alerts).forEach(([key, values]) => {
        values.forEach(value => {
          params.append(`${key}[]`, value);
        });
      });

      const result = await axios.get(`${API_URL}/in_stocks/latest?${params}`);
      
      setStockData(result.data.data);
      setTotalPages(result.data.totalPages);
      setTotalCount(result.data.totalCount);
    } catch (error) {
      console.error("Error fetching stock data:", error);
    } finally {
      setIsLoading(false);
    }
  }, [selectedDate, page, pageSize, sortConfig, filters]);

useEffect(() => {
  const loadData = async () => {
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

      // Add numerical filters
      Object.entries(filters.numerical).forEach(([key, value]) => {
        if (value?.min !== undefined) params.append(`min_${key}`, value.min);
        if (value?.max !== undefined) params.append(`max_${key}`, value.max);
      });

      // Add text filters
      Object.entries(filters.text).forEach(([key, value]) => {
        if (value) params.append(key, value);
      });

      // Add alert state filters
      Object.entries(filters.alerts).forEach(([key, values]) => {
        values.forEach(value => {
          params.append(`${key}[]`, value);
        });
      });

      const result = await axios.get(`${API_URL}/in_stocks/latest?${params}`);
      
      setStockData(result.data.data);
      setTotalPages(result.data.totalPages);
      setTotalCount(result.data.totalCount);
    } catch (error) {
      console.error("Error fetching stock data:", error);
    } finally {
      setIsLoading(false);
    }
  };

  // Reset to page 1 only when filters or date changes
  if ((filters !== prevFilters.current || selectedDate !== prevDate.current) && page !== 1) {
    prevFilters.current = filters;
    prevDate.current = selectedDate;
    setPage(1);
    return; // Don't fetch yet, let the page change trigger the fetch
  }

  loadData();
}, [filters, selectedDate, page, pageSize, sortConfig]);

  // Filter handlers
  const handleNumericFilterChange = (column, value, type) => {
    setPendingFilters(prev => ({
      ...prev,
      numerical: {
        ...prev.numerical,
        [column]: {
          ...(prev.numerical[column] || {}),
          [type]: value === '' ? undefined : value
        }
      }
    }));
    setHasFilterChanges(true);
  };

  const handleTextFilterChange = (column, value) => {
    setPendingFilters(prev => ({
      ...prev,
      text: {
        ...prev.text,
        [column]: value
      }
    }));
    setHasFilterChanges(true);
  };

  const handleAlertStateFilterChange = (column, value) => {
    setPendingFilters(prev => ({
      ...prev,
      alerts: {
        ...prev.alerts,
        [column]: prev.alerts[column].includes(value)
          ? prev.alerts[column].filter(v => v !== value)
          : [...prev.alerts[column], value]
      }
    }));
    setHasFilterChanges(true);
  };

  const applyPendingFilters = () => {
    const newFilters = { ...pendingFilters };
    setFilters(newFilters);
    saveFilters(newFilters);
    setHasFilterChanges(false);
  };

  const clearAllFilters = () => {
    const emptyFilters = {
      numerical: {},
      text: {},
      alerts: {...initialAlertStates}
    };
    setPendingFilters(emptyFilters);
    setFilters(emptyFilters);
    saveFilters(emptyFilters);
    setHasFilterChanges(false);
  };

  // Handler functions
  const handlePageChange = (event, newPage) => {
    setPage(newPage);
  };

  const handlePageSizeChange = (event) => {
    const newPageSize = event.target.value;
    setPageSize(newPageSize);
    localStorage.setItem(STORAGE_KEYS.PAGE_SIZE, newPageSize.toString());
    setPage(1);
  };

  const handleColumnVisibilityChange = (event) => {
    const { value } = event.target;
    const newHiddenColumns = typeof value === 'string' ? value.split(',') : value;
    setHiddenColumns(newHiddenColumns);
    localStorage.setItem(STORAGE_KEYS.HIDDEN_COLUMNS, JSON.stringify(newHiddenColumns));
  };

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const visibleColumns = Object.keys(columnMap).filter(
    column => !hiddenColumns.includes(column)
  );
const drawer = (
    <Box sx={{ p: 2 }}>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mb: 2, mt: 8 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">Filters & Settings</Typography>
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
        {/* Text Filters */}
        <ListItem sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
          <Typography variant="body2" sx={{ mb: 1, fontWeight: 'bold' }}>
            Text Filters
          </Typography>
          {textFilterColumns.map((column) => (
            <Box key={column} sx={{ mb: 2 }}>
              <TextField
                fullWidth
                size="small"
                label={columnMap[column]}
                placeholder={`Filter ${columnMap[column]}`}
                value={pendingFilters.text[column] || ''}
                onChange={(e) => handleTextFilterChange(column, e.target.value)}
              />
            </Box>
          ))}
        </ListItem>

        {/* Alert State Filters */}
        {Object.entries(initialAlertStates).map(([column]) => (
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
                      checked={pendingFilters.alerts[column].includes(option)}
                      onChange={() => handleAlertStateFilterChange(column, option)}
                    />
                  }
                  label={option}
                />
              ))}
            </FormGroup>
          </ListItem>
        ))}

        {/* Column Visibility */}
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

        {/* Page Size Selection */}
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
    </Box>
  );

  return (
    <Box sx={{ 
      display: 'flex', 
      flexDirection: 'column', 
      height: 'calc(100vh - 64px)',
      overflow: 'hidden',
      position: 'relative'
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
        {/* Active Filters Indicator */}
        {hasActiveFilters() && (
          <Box sx={{ 
            p: 1,
            bgcolor: 'primary.light',
            color: 'primary.contrastText',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 2
          }}>
            <Typography variant="body2">
              Filters are currently active
            </Typography>
          </Box>
        )}

        {isLoading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
            <CircularProgress />
          </Box>
        ) : (
          <TableContainer 
            component={Paper} 
            sx={{ 
              flexGrow: 1, 
              overflow: 'auto',
              '& .MuiTableHead-root': {
                position: 'sticky',
                top: 0,
                zIndex: 2,
                backgroundColor: '#f5f5f5'
              }
            }}
          >
          <Table>
            <TableHead>
              <TableRow>
                {visibleColumns.map((key) => (
                  <TableCell 
                    key={key}
                    align={key === 'stock' || key === 'stock_name' ? "left" : "center"}
                    sx={{ 
                      padding: '8px 12px',
                      fontSize: '0.9rem',
                      fontWeight: 'bold',
                      backgroundColor: '#f5f5f5',
                      borderRight: '1px solid rgba(224, 224, 224, 1)',
                      '&:last-child': {
                        borderRight: 'none'
                      },
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
                        <IconButton size="small" onClick={() => requestSort(key)}>
                          {sortConfig.key === key ? (
                            sortConfig.direction === 'ascending' ? (
                              <ArrowUpwardIcon fontSize="small" />
                            ) : (
                              <ArrowDownwardIcon fontSize="small" />
                            )
                          ) : (
                            <ArrowUpwardIcon fontSize="small" color="disabled" />
                          )}
                        </IconButton>
                      )}
                    </Box>
                  </TableCell>
                ))}
              </TableRow>
              <TableRow>
                {visibleColumns.map((key) => (
                  <TableCell 
                    key={`filter-${key}`}
                    align="center"
                    sx={{ 
                      padding: '4px 8px',
                      backgroundColor: '#f5f5f5',
                      borderRight: '1px solid rgba(224, 224, 224, 1)',
                      '&:last-child': {
                        borderRight: 'none'
                      }
                    }}
                  >
                    {filterColumns.includes(key) && numericalColumns.includes(key) && (
                      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                        <TextField
                          placeholder="Min"
                          size="small"
                          type="number"
                          value={pendingFilters.numerical[key]?.min || ''}
                          onChange={(e) => handleNumericFilterChange(key, e.target.value, 'min')}
                          sx={{ 
                            '& .MuiInputBase-input': { 
                              padding: '4px 8px',
                              fontSize: '0.75rem'
                            }
                          }}
                        />
                        <TextField
                          placeholder="Max"
                          size="small"
                          type="number"
                          value={pendingFilters.numerical[key]?.max || ''}
                          onChange={(e) => handleNumericFilterChange(key, e.target.value, 'max')}
                          sx={{ 
                            '& .MuiInputBase-input': { 
                              padding: '4px 8px',
                              fontSize: '0.75rem'
                            }
                          }}
                        />
                      </Box>
                    )}
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
                        borderRight: '1px solid rgba(224, 224, 224, 1)',
                        '&:last-child': {
                          borderRight: 'none'
                        },
                        ...(column === 'stock_name' && {
                          maxWidth: '200px',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis'
                        })
                      }}
                    >
                      {column === 'stock' ? (
                        <Link 
                          to={`/in_stock/${stock[column]}`}
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
        </TableContainer>
      )}


        <Box sx={{ 
          borderTop: 1,
          borderColor: 'divider',
          backgroundColor: 'background.paper'
        }}>
          {/* Filter Action Buttons */}
          {(hasFilterChanges || hasActiveFilters()) && (
            <Box sx={{
              p: 1,
              display: 'flex',
              justifyContent: 'flex-end',
              gap: 2,
              borderBottom: 1,
              borderColor: 'divider'
            }}>
              {hasActiveFilters() && (
                <Button
                  variant="outlined"
                  color="secondary"
                  size="small"
                  onClick={clearAllFilters}
                  startIcon={<ClearIcon />}
                >
                  Clear Filters
                </Button>
              )}
              {hasFilterChanges && (
                <Button
                  variant="contained"
                  color="primary"
                  size="small"
                  onClick={applyPendingFilters}
                  startIcon={<FilterAltIcon />}
                >
                  Apply Filters
                </Button>
              )}
            </Box>
          )}

          {/* Pagination Section */}
          <Box sx={{ 
            p: 2, 
            display: 'flex', 
            justifyContent: 'space-between',
            alignItems: 'center',
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
    </Box>
  );
}

export default IN_StockApp;