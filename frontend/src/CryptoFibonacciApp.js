import React, { useState, useEffect } from 'react';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import { 
  Table, TableBody, TableCell, TableHead, TableRow, Paper,
  TextField, Button, Typography, Box, Drawer, List, ListItem,
  Divider, useMediaQuery, useTheme, Grid, Checkbox, FormGroup, FormControlLabel,
  Tooltip, Select, MenuItem, OutlinedInput, TableContainer, Tabs, Tab
} from '@mui/material';
import { CircularProgress } from '@mui/material';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import { Link } from 'react-router-dom';

const API_URL = process.env.REACT_APP_API_URL || '/api';

const columnMap = {
  'stock': 'Crypto',
  'crypto_name': 'Crypto Name',
  'current_price': 'Current Price',
  'all_time_high': 'ATH',
  'all_time_low': 'ATL',
  '23.6': '23.6% Level',
  '23.6_distance': '23.6% Distance',
  '38.2': '38.2% Level',
  '38.2_distance': '38.2% Distance',
  '50.0': '50.0% Level',
  '50.0_distance': '50.0% Distance',
  '61.8': '61.8% Level',
  '61.8_distance': '61.8% Distance',
  '78.6': '78.6% Level',
  '78.6_distance': '78.6% Distance',
  '100.0': '100.0% Level',
  '100.0_distance': '100.0% Distance',
  '161.8': '161.8% Level',
  '161.8_distance': '161.8% Distance',
  '261.8': '261.8% Level',
  '261.8_distance': '261.8% Distance'
};

const numericalColumns = [
  'current_price', 'all_time_high', 'all_time_low',
  '23.6', '38.2', '50.0', '61.8', '78.6', '100.0', '161.8', '261.8',
  '23.6_distance', '38.2_distance', '50.0_distance', '61.8_distance', '78.6_distance', '100.0_distance', '161.8_distance', '261.8_distance'
];

const filterColumns = [
  'crypto_name',
  'current_price', 
  'all_time_high', 
  'all_time_low'
];

const drawerWidth = 300;

const formatPrice = (value, base) => {
  if (value === null || value === undefined) return 'N/A';
  switch(base) {
    case 'USD':
      return new Intl.NumberFormat('en-US', { 
        style: 'currency', 
        currency: 'USD',
        maximumFractionDigits: 2 
      }).format(value);
    case 'BTC':
      return `${Number(value).toFixed(8)} BTC`;
    case 'ETH':
      return `${Number(value).toFixed(6)} ETH`;
    default:
      return value.toFixed(6);
  }
};

const formatDistance = (value) => {
  if (value === null || value === undefined) return 'N/A';
  return `${value > 0 ? '+' : ''}${value.toFixed(2)}%`;
};

const CryptoFibonacciApp = ({ drawerOpen, toggleDrawer }) => {
  const [data, setData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [filters, setFilters] = useState({});
  const [isLoading, setIsLoading] = useState(true);
  const [selectedBase, setSelectedBase] = useState('usd');
  const [selectedDate, setSelectedDate] = useState(new Date(new Date().setDate(new Date().getDate() - 1)));
  const [sortConfig, setSortConfig] = useState({ key: 'stock', direction: 'ascending' });
  const [hiddenColumns, setHiddenColumns] = useState([]);
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        const formattedDate = selectedDate.toISOString().split('T')[0];
        const response = await fetch(`${API_URL}/crypto/fibonacci?base=${selectedBase}&date=${formattedDate}`);
        const jsonData = await response.json();
        
        const transformedData = jsonData.map(item => ({
          stock: item.stock,
          crypto_name: item.crypto_name,
          current_price: item.current_price,
          all_time_high: item.all_time_high,
          all_time_low: item.all_time_low,
          ...Object.entries(item.fibonacci_levels).reduce((acc, [level, value]) => ({
            ...acc,
            [level]: value
          }), {}),
          ...Object.entries(item.level_distances).reduce((acc, [level, value]) => ({
            ...acc,
            [`${level}_distance`]: value
          }), {})
        }));

        setData(transformedData);
        setFilteredData(transformedData);
      } catch (error) {
        console.error('Error fetching Fibonacci data:', error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [selectedDate, selectedBase]);

  const handleFilterChange = (column, value, type = null) => {
    if (column === 'crypto_name') {
      setFilters(prevFilters => ({
        ...prevFilters,
        [column]: value
      }));
    } else {
      setFilters(prevFilters => ({
        ...prevFilters,
        [column]: { ...prevFilters[column], [type]: value }
      }));
    }
  };

  const applyFilters = () => {
    const filtered = data.filter(crypto => {
      return Object.entries(filters).every(([column, value]) => {
        // Handle text filter for crypto_name
        if (column === 'crypto_name') {
          if (!value || !value.trim()) return true;
          return crypto[column].toLowerCase().includes(value.toLowerCase());
        }
        
        // Handle numeric filters
        const { min, max } = value || {};
        const numValue = parseFloat(crypto[column]);
        if (min && max) return numValue >= min && numValue <= max;
        if (min) return numValue >= min;
        if (max) return numValue <= max;
        return true;
      });
    });
    setFilteredData(filtered);
    if (isMobile) toggleDrawer();
  };

  const clearFilters = () => {
    setFilters({});
    setFilteredData(data);
  };

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const handleColumnVisibilityChange = (event) => {
    setHiddenColumns(
      typeof event.target.value === 'string' ? 
      event.target.value.split(',') : 
      event.target.value
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
            {column === 'crypto_name' ? (
              <TextField
                fullWidth
                size="small"
                placeholder="Filter by name"
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
        <Box sx={{ p: 3 }}>
          <Typography variant="h4" gutterBottom>
            Fibonacci Analysis
          </Typography>
          <Tabs
            value={selectedBase}
            onChange={(_, newValue) => setSelectedBase(newValue)}
            aria-label="base currency tabs"
            sx={{ mb: 2 }}
          >
            <Tab label="USD" value="usd" />
            <Tab label="ETH" value="eth" />
            <Tab label="BTC" value="btc" />
          </Tabs>
        </Box>

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
                      align={key === 'stock' || key === 'crypto_name' ? "left" : "center"}
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
                        justifyContent: key === 'stock' || key === 'crypto_name' ? "flex-start" : "center"
                      }}>
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
                {sortedData.map((row, index) => (
                  <TableRow key={index} hover>
                    {visibleColumns.map((column) => (
                      <TableCell 
                        key={column}
                        align={column === 'stock' || column === 'crypto_name' ? "left" : "center"}
                        sx={{ 
                          whiteSpace: 'nowrap', 
                          padding: '8px 12px',
                          fontSize: '0.85rem'
                        }}
                      >
                        {column === 'stock' ? (
                          <Link 
                            to={`/crypto/${row[column]}`}
                            style={{ 
                              color: '#1976d2', 
                              textDecoration: 'none',
                              '&:hover': { textDecoration: 'underline' }
                            }}
                          >
                            {row[column]}
                          </Link>
                        ) : column === 'crypto_name' ? (
                          row[column]
                        ) : column.endsWith('_distance') ? (
                          <Box sx={{
                            color: row[column] > 0 ? 'success.main' : 'error.main'
                          }}>
                            {formatDistance(row[column])}
                          </Box>
                        ) : (
                          formatPrice(row[column], selectedBase.toUpperCase())
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
};

export default CryptoFibonacciApp;