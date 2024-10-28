import React, { useState, useEffect } from 'react';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { CircularProgress, LinearProgress } from '@mui/material';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import { format } from 'date-fns';
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
  'stock': 'Crypto',
  'crypto_name': 'Crypto Name',
  'close': 'Last Price',
  'ema': '200-EMA',
  'williams_r': 'Williams %R',
  'williams_r_ema': 'Williams %R EMA',
  'williams_r_momentum_alert_state': 'Williams %R Momentum Alert',
  'force_index_7_week': '7-Week Force Index',
  'force_index_52_week': '52-Week Force Index',
  'force_index_alert_state': 'Force Index Alert',
  'williams_r_rank': 'Williams %R Rank',
  'williams_r_ema_rank': 'Williams %R EMA Rank',
  'force_index_7_week_rank': '7-Week Force Index Rank',
  'force_index_52_week_rank': '52-Week Force Index Rank',
  'ema_rank': '200-EMA Rank',
  'datetime': 'Time'
};

const numericalColumns = [
  'close', 'ema', 'williams_r', 'williams_r_ema', 'force_index_7_week', 'force_index_52_week', 'williams_r_rank', 'williams_r_ema_rank',
  'force_index_7_week_rank', 'force_index_52_week_rank', 'ema_rank'
];

const filterColumns = [
  'close', 'ema', 'williams_r', 'williams_r_ema', 'force_index_7_week', 'force_index_52_week', 'williams_r_rank', 'williams_r_ema_rank',
  'force_index_7_week_rank', 'force_index_52_week_rank', 'ema_rank'
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

const formatColumnValue = (column, value) => {
  switch (column) {
    case 'close':
      return formatCurrency(value);
    case 'ema':
    case 'williams_r':
    case 'williams_r_ema':
    case 'force_index_7_week':
    case 'force_index_52_week':
      return formatRatio(value);
    case 'datetime':
      return new Date(value).toLocaleString();
    case 'williams_r_rank':
    case 'williams_r_ema_rank':
    case 'force_index_7_week_rank':
    case 'force_index_52_week_rank':
    case 'ema_rank':
      return formatRank(value);
    default:
      return value;
  }
};

function CryptoApp({ drawerOpen, toggleDrawer }) {
  const [cryptoData, setCryptoData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [filters, setFilters] = useState({});
  const [isLoading, setIsLoading] = useState(true);
  const [selectedDate, setSelectedDate] = useState(new Date(new Date().setDate(new Date().getDate() - 1)));
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
        setIsLoading(true);  // Start loading
        const formattedDate = selectedDate.toISOString().split('T')[0];
        const result = await axios.get(`${API_URL}/crypto/latest?date=${formattedDate}`);
        setCryptoData(result.data);
        setFilteredData(result.data);
      } catch (error) {
        console.error("Error fetching crypto data:", error);
      } finally {
        setIsLoading(false);  // End loading regardless of success/failure
      }
    };

    fetchData();
  }, [selectedDate]);
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
    const filtered = cryptoData.filter(crypto => {
      return Object.entries(filters).every(([column, { min, max }]) => {
        const value = parseFloat(crypto[column]);
        if (min && max) return value >= min && value <= max;
        if (min) return value >= min;
        if (max) return value <= max;
        return true;
      }) && Object.entries(alertStateFilters).every(([column, selectedStates]) => {
        return selectedStates.length === 0 || selectedStates.includes(crypto[column]);
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
    setFilteredData(cryptoData);
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
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mb: 2, mt: 8 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">Filters</Typography>
          <Button onClick={toggleDrawer}>
            <ChevronLeftIcon />
          </Button>
        </Box>
        <LocalizationProvider dateAdapter={AdapterDateFns}>
          <DatePicker
            label="Select Date"
            value={selectedDate}
            onChange={(newDate) => {
              if (newDate) {
                setSelectedDate(newDate);
              }
            }}
            maxDate={new Date(new Date().setDate(new Date().getDate() - 1))}
            slotProps={{ textField: { size: "small", fullWidth: true } }}
          />
        </LocalizationProvider>
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
          )}
        </TableContainer>
      </Box>
    </Box>
  );
}

export default CryptoApp;