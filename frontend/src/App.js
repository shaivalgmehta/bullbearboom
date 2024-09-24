import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  Table, TableBody, TableCell, TableHead, TableRow, Paper,
  TextField, Button, Typography, Box, Drawer, IconButton, List, ListItem,
  Divider, useMediaQuery, useTheme, Grid, Checkbox, FormGroup, FormControlLabel
} from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';

const API_URL = process.env.REACT_APP_API_URL || '/api';

const columnMap = {
  'stock': 'Stock',
  'stock_name': 'Stock Name',
  'market_cap': 'Market Cap',
  'closing_price': 'Last Day Closing Price',
  'pe_ratio': 'P/E Ratio',
  'ev_ebitda': 'EV/EBITDA',
  'pb_ratio': 'P/B Ratio',
  'peg_ratio': 'PEG Ratio',
  'current_year_sales': 'Current Year Sales',
  'current_year_ebitda': 'Current Year EBITDA',
  'ema': '200-EMA',
  'williams_r': 'Williams %R',
  'williams_r_ema': 'Williams %R EMA',
  'williams_r_momentum_alert_state': 'Williams %R Momentum Alert',
  'force_index_7_week': '7-Week Force Index',
  'force_index_52_week': '52-Week Force Index',
  'force_index_alert_state': 'Force Index Alert',
  'time': 'Time'
};

const numericalColumns = [
  'market_cap', 'closing_price', 'pe_ratio', 'ev_ebitda', 'pb_ratio', 
  'peg_ratio', 'current_year_sales', 'current_year_ebitda', 'ema',
  'williams_r', 'williams_r_ema', 'force_index_7_week', 'force_index_52_week'
];

const filterColumns = [
  'market_cap', 'pe_ratio', 'ev_ebitda', 'pb_ratio', 
  'peg_ratio', 'current_year_sales', 'current_year_ebitda', 'ema'
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

const formatPercentage = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return `${(Number(value) * 100).toFixed(2)}%`;
};

const formatColumnValue = (column, value) => {
  switch (column) {
    case 'market_cap':
    case 'current_year_sales':
    case 'current_year_ebitda':
    case 'closing_price':
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
    case 'time':
      return new Date(value).toLocaleString();
    default:
      return value;
  }
};

function App() {
  const [stockData, setStockData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [filters, setFilters] = useState({});
  const [alertStateFilters, setAlertStateFilters] = useState({
    williams_r_momentum_alert_state: [],
    force_index_alert_state: []
  });
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'ascending' });
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
    if (isMobile) setDrawerOpen(false);
  };

  const clearFilters = () => {
    setFilters({});
    setAlertStateFilters({
      williams_r_momentum_alert_state: [],
      force_index_alert_state: []
    });
    setFilteredData(stockData);
  };

  const toggleDrawer = () => {
    setDrawerOpen(!drawerOpen);
  };

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

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
        <IconButton onClick={toggleDrawer}>
          <ChevronLeftIcon />
        </IconButton>
      </Box>
      <Divider />
      <List>
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
    <Box sx={{ display: 'flex', flexDirection: 'column', p: 2 }}>
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
          width: '100%',
          transition: theme.transitions.create(['margin', 'width'], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.leavingScreen,
          }),
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <IconButton
            color="inherit"
            aria-label="open drawer"
            edge="start"
            onClick={toggleDrawer}
            sx={{ mr: 2, ...(drawerOpen && { display: 'none' }) }}
          >
            <MenuIcon />
          </IconButton>
          <Typography variant="h4" component="h1">
            Bull Bear Boom
          </Typography>
        </Box>

        <Table component={Paper} sx={{ width: '100%', overflowX: 'unset' }}>
          <TableHead>
            <TableRow>
              {Object.entries(columnMap).map(([key, value]) => (
                <TableCell 
                  key={key} 
                  align={key === 'stock' || key === 'stock_name' ? "left" : "center"}
                  sx={{ 
                    whiteSpace: 'nowrap', 
                    padding: '8px 12px',
                    fontSize: '0.9rem',
                    fontWeight: 'bold',
                    backgroundColor: '#f8f9fa',
                    position: 'sticky',
                    top: 0,
                    zIndex: 1
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: key === 'stock' || key === 'stock_name' ? "flex-start" : "center" }}>
                    {value}
                    {numericalColumns.includes(key) && (
                      <IconButton size="small" onClick={() => requestSort(key)}>
                        {sortConfig.key === key ? (
                          sortConfig.direction === 'ascending' ? (
                            <ArrowUpwardIcon fontSize="inherit" />
                          ) : (
                            <ArrowDownwardIcon fontSize="inherit" />
                          )
                        ) : (
                          <ArrowUpwardIcon fontSize="inherit" color="disabled" />
                        )}
                      </IconButton>
                    )}
                  </Box>
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {sortedData.map((stock, index) => (
              <TableRow key={index} hover>
                {Object.keys(columnMap).map((column, colIndex) => (
                  <TableCell 
                    key={colIndex} 
                    align={column === 'stock' || column === 'stock_name' ? "left" : "center"}
                    sx={{ 
                      whiteSpace: 'nowrap', 
                      padding: '8px 12px',
                      fontSize: '0.85rem'
                    }}
                  >
                    {formatColumnValue(column, stock[column])}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Box>
    </Box>
  );
}

export default App;