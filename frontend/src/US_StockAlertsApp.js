import React, { useContext, useState, useEffect } from 'react';
import { CircularProgress } from '@mui/material';
import axios from 'axios';
import { 
  Table, TableBody, TableCell, TableHead, TableRow, Paper,
  TextField, Button, Typography, Box, Drawer, List, ListItem,
  Divider, useMediaQuery, useTheme, Checkbox, FormGroup, FormControlLabel,
  Tooltip, Select, MenuItem, OutlinedInput, TableContainer,
  Collapse, IconButton, Chip
} from '@mui/material';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import KeyboardArrowUpIcon from '@mui/icons-material/KeyboardArrowUp';
import Switch from '@mui/material/Switch';
import { Link } from 'react-router-dom';
import WatchListStar from './Watchlist/WatchListStar';
import { AuthContext } from './Authentication/AuthContext';

const API_URL = process.env.REACT_APP_API_URL || '/api';

const columnMap = {
  'stock': 'Stock Symbol',
  'stock_name': 'Stock Name',
  'alerts': 'Alerts',
  'datetime': 'Date'
};

const filterColumns = ['stock', 'stock_name'];

// Updated to include Heikin-Ashi alert types
const alertTypeOptions = [
  'oversold', 
  'obv_positive', 
  'obv_negative', 
  'heikin_ashi_3d_bullish', 
  'heikin_ashi_3d_bearish', 
  'heikin_ashi_2w_bullish', 
  'heikin_ashi_2w_bearish'
];

const drawerWidth = 300;

function US_StockAlertsApp({ drawerOpen, toggleDrawer }) {
  const [alertsData, setAlertsData] = useState([]);
  const [groupedData, setGroupedData] = useState({});
  const [expandedDates, setExpandedDates] = useState({});
  const [filteredData, setFilteredData] = useState({});
  const [filters, setFilters] = useState({});
  const [isLoading, setIsLoading] = useState(true);
  const [alertTypeFilters, setAlertTypeFilters] = useState([]);
  const [sortConfig, setSortConfig] = useState({ key: 'datetime', direction: 'descending' });
  const [hiddenColumns, setHiddenColumns] = useState([]);
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));
  const [watchListOnly, setWatchListOnly] = useState(false);
  const { user } = useContext(AuthContext);


  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        // Add watch_list_only parameter to API call
        const params = new URLSearchParams();
        if (watchListOnly) {
          params.append('watch_list_only', 'true');
        }

        const result = await axios.get(`${API_URL}/stocks/alerts${params.toString() ? `?${params.toString()}` : ''}`);
        
        // Group by date
        const grouped = result.data.reduce((acc, alert) => {
          const date = new Date(alert.datetime).toLocaleDateString();
          if (!acc[date]) {
            acc[date] = [];
          }
          acc[date].push(alert);
          return acc;
        }, {});
        
        setAlertsData(result.data);
        setGroupedData(grouped);
        setFilteredData(grouped);
        
        // Initialize expanded state for all dates
        const expandedState = Object.keys(grouped).reduce((acc, date) => {
          acc[date] = false;
          return acc;
        }, {});
        setExpandedDates(expandedState);
      } catch (error) {
        console.error("Error fetching alerts data:", error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [watchListOnly]); // Add watchListOnly to dependency array

  const handleFilterChange = (column, value) => {
    setFilters(prevFilters => ({
      ...prevFilters,
      [column]: value.toLowerCase()
    }));
  };

  const handleAlertTypeFilterChange = (type) => {
    setAlertTypeFilters(prevFilters => {
      if (prevFilters.includes(type)) {
        return prevFilters.filter(t => t !== type);
      } else {
        return [...prevFilters, type];
      }
    });
  };

  const applyFilters = () => {
    const filtered = {};
    
    // Apply text filters and alert type filters
    Object.entries(groupedData).forEach(([date, alerts]) => {
      const filteredAlerts = alerts.filter(alert => {
        // Apply text-based filters
        const matchesTextFilters = Object.entries(filters).every(([column, value]) => {
          if (!value) return true;
          if (!alert[column]) return false;
          return String(alert[column]).toLowerCase().includes(value);
        });

        // Apply alert type filters
        const matchesAlertTypes = alertTypeFilters.length === 0 || 
          alertTypeFilters.every(type => {
            return alert.alerts && alert.alerts.some(a => a.type === type);
          });

        return matchesTextFilters && matchesAlertTypes;
      });
      
      if (filteredAlerts.length > 0) {
        filtered[date] = filteredAlerts;
      }
    });
    
    setFilteredData(filtered);
    if (isMobile) toggleDrawer();
  };

  const clearFilters = () => {
    setFilters({});
    setAlertTypeFilters([]);
    setFilteredData(groupedData);
  };

  const toggleDateExpansion = (date) => {
    setExpandedDates(prev => ({
      ...prev,
      [date]: !prev[date]
    }));
  };

  const expandAllDates = () => {
    const newExpandedState = {};
    Object.keys(filteredData).forEach(date => {
      newExpandedState[date] = true;
    });
    setExpandedDates(newExpandedState);
  };

  const collapseAllDates = () => {
    const newExpandedState = {};
    Object.keys(filteredData).forEach(date => {
      newExpandedState[date] = false;
    });
    setExpandedDates(newExpandedState);
  };

  const handleColumnVisibilityChange = (event) => {
    const { value } = event.target;
    setHiddenColumns(typeof value === 'string' ? value.split(',') : value);
  };

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const sortedDates = React.useMemo(() => {
    return Object.keys(filteredData).sort((a, b) => {
      const dateA = new Date(a);
      const dateB = new Date(b);
      return sortConfig.direction === 'ascending' 
        ? dateA - dateB 
        : dateB - dateA;
    });
  }, [filteredData, sortConfig]);

  const visibleColumns = Object.keys(columnMap).filter(column => !hiddenColumns.includes(column));

  // Function to determine alert color
  const getAlertColor = (alertType) => {
    switch (alertType) {
      case 'oversold':
        return '#4caf50'; // Green
      case 'obv_positive':
        return '#9c27b0'; // Purple
      case 'obv_negative':
        return '#f44336'; // Red
      case 'heikin_ashi_3d_bullish':
        return '#00BCD4'; // Cyan
      case 'heikin_ashi_3d_bearish':
        return '#FF9800'; // Orange
      case 'heikin_ashi_2w_bullish':
        return '#2196F3'; // Blue
      case 'heikin_ashi_2w_bearish':
        return '#FF5722'; // Deep Orange
      default:
        return '#757575'; // Grey
    }
  };

  // Function to get alert label
  const getAlertLabel = (alertType) => {
    switch (alertType) {
      case 'oversold':
        return 'Oversold';
      case 'obv_positive':
        return 'OBV+';
      case 'obv_negative':
        return 'OBV-';
      case 'heikin_ashi_3d_bullish':
        return 'HA-3D+';
      case 'heikin_ashi_3d_bearish':
        return 'HA-3D-';
      case 'heikin_ashi_2w_bullish':
        return 'HA-2W+';
      case 'heikin_ashi_2w_bearish':
        return 'HA-2W-';
      default:
        return alertType;
    }
  };

  // Function to get detailed alert description for tooltip
  const getAlertDescription = (alertType, description) => {
    if (description) return description;
    
    switch (alertType) {
      case 'oversold':
        return 'Stock is potentially oversold based on Williams %R and Force Index indicators';
      case 'obv_positive':
        return 'On-Balance Volume (OBV) shows positive momentum';
      case 'obv_negative':
        return 'On-Balance Volume (OBV) shows negative momentum';
      case 'heikin_ashi_3d_bullish':
        return '3-day Heikin-Ashi pattern shows bullish reversal (red to green)';
      case 'heikin_ashi_3d_bearish':
        return '3-day Heikin-Ashi pattern shows bearish reversal (green to red)';
      case 'heikin_ashi_2w_bullish':
        return '2-week Heikin-Ashi pattern shows bullish reversal (red to green)';
      case 'heikin_ashi_2w_bearish':
        return '2-week Heikin-Ashi pattern shows bearish reversal (green to red)';
      default:
        return alertType;
    }
  };

  const drawer = (
    <Box sx={{ p: 2 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2, mt: 8 }}>
        <Typography variant="h6">Filters</Typography>
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
            <TextField
              fullWidth
              size="small"
              placeholder={`Filter ${columnMap[column]}`}
              onChange={(e) => handleFilterChange(column, e.target.value)}
              value={filters[column] || ''}
            />
          </ListItem>
        ))}

        <ListItem sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
          <Typography variant="body2" sx={{ mb: 1, fontWeight: 'bold' }}>
            Alert Types
          </Typography>
          <FormGroup>
            {alertTypeOptions.map((option) => (
              <FormControlLabel
                key={option}
                control={
                  <Checkbox
                    checked={alertTypeFilters.includes(option)}
                    onChange={() => handleAlertTypeFilterChange(option)}
                  />
                }
                label={
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <span>{getAlertLabel(option)}</span>
                    <Chip 
                      size="small" 
                      sx={{ 
                        backgroundColor: getAlertColor(option),
                        color: 'white',
                        fontSize: '0.6rem',
                        height: 16,
                        '& .MuiChip-label': { px: 0.5 }
                      }}
                    />
                  </Box>
                }
              />
            ))}
          </FormGroup>
        </ListItem>
        <ListItem sx={{ flexDirection: 'column', alignItems: 'stretch', mb: 2 }}>
          <Typography variant="body2" sx={{ mb: 1, fontWeight: 'bold' }}>
            Watch List
          </Typography>
          <FormControlLabel
            control={
              <Switch
                checked={watchListOnly}
                onChange={(e) => setWatchListOnly(e.target.checked)}
                disabled={!user}
              />
            }
            label="Show Watch List Only"
          />
          {!user && (
            <Typography variant="caption" color="text.secondary">
              Log in to use watch list features
            </Typography>
          )}
        </ListItem>   
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
        <Box sx={{ 
          display: 'flex', 
          justifyContent: 'space-between', 
          alignItems: 'center',
          p: 2,
          borderBottom: 1,
          borderColor: 'divider'
        }}>
          <Typography variant="h6">
            Stock Alerts {sortedDates.length > 0 ? `(${sortedDates.length} days)` : ''}
          </Typography>
          <Box>
            <Button onClick={expandAllDates} sx={{ mr: 1 }}>Expand All</Button>
            <Button onClick={collapseAllDates}>Collapse All</Button>
          </Box>
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
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ width: '3%' }}></TableCell>
                  <TableCell 
                    sx={{ 
                      fontWeight: 'bold',
                      fontSize: '1rem',
                      backgroundColor: '#f8f9fa'
                    }}
                  >
                    <Box sx={{ 
                      display: 'flex', 
                      alignItems: 'center'
                    }}>
                      {columnMap['datetime']}
                      <Button size="small" onClick={() => requestSort('datetime')}>
                        {sortConfig.key === 'datetime' ? (
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
                  <TableCell 
                    sx={{ 
                      fontWeight: 'bold',
                      fontSize: '1rem',
                      backgroundColor: '#f8f9fa'
                    }}
                  >
                    Alerts Count
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {sortedDates.length > 0 ? (
                  sortedDates.map((date) => (
                    <React.Fragment key={date}>
                      <TableRow 
                        sx={{ 
                          '& > *': { borderBottom: 'unset' },
                          cursor: 'pointer'
                        }}
                        onClick={() => toggleDateExpansion(date)}
                      >
                        <TableCell>
                          <IconButton
                            aria-label="expand row"
                            size="small"
                          >
                            {expandedDates[date] ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
                          </IconButton>
                        </TableCell>
                        <TableCell component="th" scope="row">
                          <Typography variant="body1" fontWeight="bold">
                            {date}
                          </Typography>
                        </TableCell>
                        <TableCell>
                          <Chip 
                            label={filteredData[date].length} 
                            color="primary" 
                            size="small" 
                          />
                        </TableCell>
                      </TableRow>
                      <TableRow>
                        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={3}>
                          <Collapse in={expandedDates[date]} timeout="auto" unmountOnExit>
                            <Box sx={{ margin: 1 }}>
                              <Table size="small" aria-label="alerts">
                                <TableHead>
                                  <TableRow>
                                    <TableCell />
                                    {visibleColumns.filter(col => col !== 'datetime').map((key) => (
                                      <TableCell 
                                        key={key}
                                        align={key === 'stock' || key === 'stock_name' ? "left" : "center"}
                                        sx={{ 
                                          whiteSpace: 'nowrap', 
                                          padding: '8px 12px',
                                          fontSize: '0.9rem',
                                          fontWeight: 'bold',
                                          backgroundColor: '#f8f9fa'
                                        }}
                                      >
                                        {columnMap[key]}
                                      </TableCell>
                                    ))}
                                  </TableRow>
                                </TableHead>
                                <TableBody>
                                  {filteredData[date].map((alert, index) => (
                                    <TableRow key={`${alert.stock}-${index}`} hover>
                                      <TableCell padding="checkbox">
                                        <WatchListStar 
                                          entityType="us_stock" 
                                          symbol={alert.stock} 
                                        />
                                      </TableCell>
                                      {visibleColumns.filter(col => col !== 'datetime').map((column) => (
                                        <TableCell 
                                          key={column}
                                          align={column === 'stock' || column === 'stock_name' ? "left" : "center"}
                                          sx={{ 
                                            whiteSpace: 'nowrap', 
                                            padding: '8px 12px',
                                            fontSize: '0.85rem'
                                          }}
                                        >
                                          {column === 'stock' ? (
                                            <Link 
                                              to={`/us_stock/${alert[column]}`}
                                              style={{ 
                                                color: '#1976d2', 
                                                textDecoration: 'none'
                                              }}
                                            >
                                              {alert[column]}
                                            </Link>
                                          ) : column === 'stock_name' ? (
                                            <Tooltip title={alert[column]} placement="top">
                                              <span>{alert[column]}</span>
                                            </Tooltip>
                                          ) : column === 'alerts' ? (
                                            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                                              {alert.alerts && alert.alerts.map((a, i) => (
                                                <Tooltip 
                                                  key={i} 
                                                  title={getAlertDescription(a.type, a.description)}
                                                  placement="top"
                                                >
                                                  <Chip
                                                    label={getAlertLabel(a.type)}
                                                    size="small"
                                                    sx={{
                                                      backgroundColor: getAlertColor(a.type),
                                                      color: 'white',
                                                      fontWeight: 'bold'
                                                    }}
                                                  />
                                                </Tooltip>
                                              ))}
                                            </Box>
                                          ) : (
                                            alert[column]
                                          )}
                                        </TableCell>
                                      ))}
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            </Box>
                          </Collapse>
                        </TableCell>
                      </TableRow>
                    </React.Fragment>
                  ))
                ) : (
                  <TableRow>
                    <TableCell colSpan={3} align="center">
                      <Typography variant="body1" sx={{ p: 2 }}>
                        No alerts found matching the filters
                      </Typography>
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          )}
        </TableContainer>
      </Box>
    </Box>
  );
}

export default US_StockAlertsApp;