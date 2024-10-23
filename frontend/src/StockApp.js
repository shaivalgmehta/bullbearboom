import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  Table, TableBody, TableCell, TableHead, TableRow, Paper,
  TextField, Button, Typography, Box, Drawer, List, ListItem,
  Divider, useMediaQuery, useTheme, Grid, Checkbox, FormGroup, FormControlLabel,
  Tooltip, Select, MenuItem, OutlinedInput, TableContainer
} from '@mui/material';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';

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
  'pe_ratio_rank': 'P/E Ratio Ranking',
  'ev_ebitda_rank': 'EV/EBITDA Ranking',
  'pb_ratio_rank': 'P/B Ratio Ranking',
  'peg_ratio_rank': 'PEG Ratio Ranking',
  'datetime': 'Time'
};

const numericalColumns = [
  'market_cap', 'close', 'pe_ratio', 'ev_ebitda', 'pb_ratio', 
  'peg_ratio', 'current_quarter_sales', 'last_quarter_sales', 'current_quarter_ebitda', 'last_quarter_ebitda', 'ema',
  'williams_r', 'williams_r_ema', 'force_index_7_week', 'force_index_52_week', 'pe_ratio_rank', 'ev_ebitda_rank',
  'pb_ratio_rank', 'peg_ratio_rank'
];

const filterColumns = [
  'market_cap', 'pe_ratio', 'ev_ebitda', 'pb_ratio', 
  'peg_ratio', 'current_quarter_sales', 'current_quarter_ebitda', 'ema', 'pe_ratio_rank', 'ev_ebitda_rank',
  'pb_ratio_rank', 'peg_ratio_rank'
];

const alertStateOptions = ['$', '$$$', '-'];

const drawerWidth = 300;

const formatCurrency = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(value);
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
    case 'ema':
    case 'williams_r':
    case 'williams_r_ema':
    case 'force_index_7_week':
    case 'force_index_52_week':
      return formatRatio(value);
    case 'sales_change_percent':
    case 'ebitda_change_percent':
      return formatPercentage(value);
    case 'pe_ratio_rank':
    case 'ev_ebitda_rank':
    case 'pb_ratio_rank':
    case 'peg_ratio_rank':
      return formatRank(value);
    case 'datetime':
      return new Date(value).toLocaleString();
    default:
      return value;
  }
};

function StockApp({ drawerOpen, toggleDrawer }) {
  const [stockData, setStockData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [filters, setFilters] = useState({});
  const [alertStateFilters, setAlertStateFilters] = useState({
    williams_r_momentum_alert_state: [],
    force_index_alert_state: []
  });
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'ascending' });
  const [hiddenColumns, setHiddenColumns] = useState([]);
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  useEffect(() => {
    const fetchData = async () => {
      try {
        const result = await axios.get(`${API_URL}/stocks/latest`);
        setStockData(result.data);
        setFilteredData(result.data);
      } catch (error) {
        console.error("Error fetching stock data:", error);
      }
    };

    fetchData();
  }, []);

  const handleFilterChange = (column, value, type) => {
    setFilters(prevFilters => ({
      ...prevFilters,
      [column]: { ...prevFilters[column], [type]: value }
    }));
  };

  const handleAlertStateFilterChange = (column, value) => {
    setAlertStateFilters(prevFilters => ({
      ...prevFilters,
      [column]: prevFilters[column].includes(value)
        ? prevFilters[column].filter(v => v !== value)
        : [...prevFilters[column], value]
    }));
  };

  const applyFilters = () => {
    const filtered = stockData.filter(stock => {
      return Object.entries(filters).every(([column, { min, max }]) => {
        const value = parseFloat(stock[column]);
        if (min && max) return value >= min && value <= max;
        if (min) return value >= min;
        if (max) return value <= max;
        return true;
      }) && Object.entries(alertStateFilters).every(([column, selectedStates]) => {
        return selectedStates.length === 0 || selectedStates.includes(stock[column]);
      });
    });
    setFilteredData(filtered);
    if (isMobile) toggleDrawer();
  };

  const clearFilters = () => {
    setFilters({});
    setAlertStateFilters({
      williams_r_momentum_alert_state: [],
      force_index_alert_state: []
    });
    setFilteredData(stockData);
  };

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const handleColumnVisibilityChange = (event) => {
    const {
      target: { value },
    } = event;
    setHiddenColumns(
      typeof value === 'string' ? value.split(',') : value,
    );
  };

  const visibleColumns = Object.keys(columnMap).filter(column => !hiddenColumns.includes(column));

  const sortedData = React.useMemo(() => {
    let sortableItems = [...filteredData];
    if (sortConfig.key !== null) {
      sortableItems.sort((a, b) => {
        if (a[sortConfig.key] < b[sortConfig.key]) {
          return sortConfig.direction === 'ascending' ? -1 : 1;
        }
        if (a[sortConfig.key] > b[sortConfig.key]) {
          return sortConfig.direction === 'ascending' ? 1 : -1;
        }
        return 0;
      });
    }
    return sortableItems;
  }, [filteredData, sortConfig]);

  const drawer = (
    <Box sx={{ p: 2 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', mb: 2 }}>
        <Button onClick={toggleDrawer}>
          <ChevronLeftIcon />
        </Button>
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
          </ListItem>
        ))}
        {['williams_r_momentum_alert_state', 'force_index_alert_state'].map((column) => (
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
                      checked={alertStateFilters[column].includes(option)}
                      onChange={() => handleAlertStateFilterChange(column, option)}
                    />
                  }
                  label={option}
                />
              ))}
            </FormGroup>
          </ListItem>
        ))}
      </List>
      <Box sx={{ mt: 2 }}>
        <Button variant="contained" fullWidth onClick={applyFilters} sx={{ mb: 1 }}>
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
      height: 'calc(100vh - 64px)', // Subtracting the AppBar height
      overflow: 'hidden', // Prevent scrolling on this container
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
                    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: key === 'stock' || key === 'stock_name' ? "flex-start" : "center" }}>
                      {columnMap[key]}
                      {numericalColumns.includes(key) && (
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
              {sortedData.map((stock, index) => (
                <TableRow key={index} hover>
                  {visibleColumns.map((column, colIndex) => (
                    <TableCell 
                      key={colIndex} 
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
                      {column === 'stock_name' ? (
                        <Tooltip title={stock[column]} placement="top">
                          <span>{formatColumnValue(column, stock[column])}</span>
                        </Tooltip>
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
      </Box>
    </Box>
  );
}

export default StockApp;