import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  Table, TableBody, TableCell, TableHead, TableRow, Paper,
  TextField, Button, Typography, Box, Drawer, List, ListItem,
  Divider, useMediaQuery, useTheme, Grid, Checkbox, FormGroup,
  Tooltip, Select, MenuItem, OutlinedInput, TableContainer, Tabs, Tab,
  CircularProgress
} from '@mui/material';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';

const API_URL = process.env.REACT_APP_API_URL || '/api';

const columnMap = {
  'stock': 'Symbol',
  'crypto_name': 'Name',
  'alert': 'Alert Type',
  'datetime': 'Date & Time'
};

const filterColumns = ['stock', 'crypto_name'];
const drawerWidth = 300;

function CryptoAlertsApp({ drawerOpen, toggleDrawer }) {
  const [alertsData, setAlertsData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [filters, setFilters] = useState({});
  const [isLoading, setIsLoading] = useState(true);
  const [selectedBase, setSelectedBase] = useState('usd');
  const [sortConfig, setSortConfig] = useState({ key: 'datetime', direction: 'descending' });
  const [hiddenColumns, setHiddenColumns] = useState([]);
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        const endpoint = selectedBase === 'usd' 
          ? 'alerts' 
          : `alerts_${selectedBase}`;
        const result = await axios.get(`${API_URL}/crypto/${endpoint}`);
        setAlertsData(result.data);
        setFilteredData(result.data);
      } catch (error) {
        console.error("Error fetching alerts data:", error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [selectedBase]);

  const handleFilterChange = (column, value) => {
    setFilters(prevFilters => ({
      ...prevFilters,
      [column]: value.toLowerCase()
    }));
  };

  const applyFilters = () => {
    const filtered = alertsData.filter(alert => {
      return Object.entries(filters).every(([column, value]) => {
        if (!value) return true;
        const alertValue = String(alert[column]).toLowerCase();
        return alertValue.includes(value);
      });
    });
    setFilteredData(filtered);
    if (isMobile) toggleDrawer();
  };

  const clearFilters = () => {
    setFilters({});
    setFilteredData(alertsData);
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

  const sortedData = React.useMemo(() => {
    let sortableItems = [...filteredData];
    if (sortConfig.key !== null) {
      sortableItems.sort((a, b) => {
        const aValue = a[sortConfig.key];
        const bValue = b[sortConfig.key];
        if (aValue < bValue) {
          return sortConfig.direction === 'ascending' ? -1 : 1;
        }
        if (aValue > bValue) {
          return sortConfig.direction === 'ascending' ? 1 : -1;
        }
        return 0;
      });
    }
    return sortableItems;
  }, [filteredData, sortConfig]);

  const formatValue = (column, value) => {
    if (column === 'datetime') {
      return new Date(value).toLocaleString();
    }
    return value;
  };

  const drawer = (
    <Box sx={{ p: 2 }}>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mb: 2, mt: 8 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">Filters</Typography>
          <Button onClick={toggleDrawer}>
            <ChevronLeftIcon />
          </Button>
        </Box>
      </Box>
      <Divider />
      <List>
        <ListItem sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
          <Typography variant="body2" sx={{ mb: 1, fontWeight: 'bold' }}>
            Base Currency
          </Typography>
          <Tabs
            value={selectedBase}
            onChange={(_, newValue) => setSelectedBase(newValue)}
            aria-label="base currency tabs"
            variant="fullWidth"
          >
            <Tab label="USD" value="usd" />
            <Tab label="ETH" value="eth" />
            <Tab label="BTC" value="btc" />
          </Tabs>
        </ListItem>
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
            <TextField
              fullWidth
              size="small"
              placeholder={`Filter ${columnMap[column]}`}
              onChange={(e) => handleFilterChange(column, e.target.value)}
              value={filters[column] || ''}
            />
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
                      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: key === 'stock' || key === 'crypto_name' ? "flex-start" : "center" }}>
                        {columnMap[key]}
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
                      </Box>
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {sortedData.map((alert, index) => (
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
                        {column === 'crypto_name' ? (
                          <Tooltip title={alert[column]} placement="top">
                            <span>{formatValue(column, alert[column])}</span>
                          </Tooltip>
                        ) : (
                          formatValue(column, alert[column])
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

export default CryptoAlertsApp;