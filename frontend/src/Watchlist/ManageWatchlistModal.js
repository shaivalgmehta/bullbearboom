import React, { useState, useEffect, useContext } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Typography,
  Box,
  List,
  ListItem,
  ListItemText,
  IconButton,
  Divider,
  CircularProgress,
  Chip,
  Tabs,
  Tab,
  Paper,
  Alert,
  Snackbar
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import DeleteIcon from '@mui/icons-material/Delete';
import axios from 'axios';
import { WatchListContext } from './WatchListContext';

const API_URL = process.env.REACT_APP_API_URL || '/api';

// Tab Panel component
function TabPanel(props) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`watchlist-tabpanel-${index}`}
      aria-labelledby={`watchlist-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 2 }}>{children}</Box>}
    </div>
  );
}

function ManageWatchlistModal({ open, onClose }) {
  const { watchList, isInWatchList, addToWatchList, removeFromWatchList } = useContext(WatchListContext);
  const [bulkInput, setBulkInput] = useState('');
  const [processing, setProcessing] = useState(false);
  const [tabValue, setTabValue] = useState(0);
  const [successCount, setSuccessCount] = useState(0);
  const [failedSymbols, setFailedSymbols] = useState([]);
  const [notification, setNotification] = useState({ open: false, message: '', severity: 'success' });

  // Group watchlist items by type
  const groupedWatchlist = {
    us_stock: watchList.filter(item => item.entity_type === 'us_stock'),
    in_stock: watchList.filter(item => item.entity_type === 'in_stock'),
    crypto_usd: watchList.filter(item => item.entity_type === 'crypto'),
    crypto_eth: watchList.filter(item => item.entity_type === 'crypto'),
    crypto_btc: watchList.filter(item => item.entity_type === 'crypto')
  };

  // Handle tab change
  const handleTabChange = (event, newValue) => {
    setTabValue(newValue);
  };

  // Handle bulk add symbols
  const handleBulkAdd = async () => {
    if (!bulkInput.trim()) {
      setNotification({
        open: true,
        message: 'Please enter at least one symbol',
        severity: 'error'
      });
      return;
    }

    setProcessing(true);
    setSuccessCount(0);
    setFailedSymbols([]);

    // Parse input and remove duplicates
    const symbols = bulkInput
      .split(/[,\s\n]+/)
      .map(symbol => symbol.trim().toUpperCase())
      .filter(symbol => symbol); // Remove empty strings

    // Determine entity type based on active tab
    let entityType;
    switch (tabValue) {
      case 0: 
        entityType = 'us_stock';
        break;
      case 1:
        entityType = 'in_stock';
        break;
      case 2:
        entityType = 'crypto';
        break;
      case 3:
        entityType = 'crypto';
        break;
      case 4:
        entityType = 'crypto';
        break;
      default:
        entityType = 'us_stock';
    }

    let successCounter = 0;
    let failedList = [];

    // Process symbols one by one
    for (const symbol of symbols) {
      try {
        // Skip if already in watchlist
        if (isInWatchList(entityType, symbol)) {
          failedList.push({ symbol, reason: 'Already in watchlist' });
          continue;
        }

        const success = await addToWatchList(entityType, symbol);
        if (success) {
          successCounter++;
        } else {
          failedList.push({ symbol, reason: 'Failed to add' });
        }
      } catch (error) {
        console.error(`Error adding ${symbol}:`, error);
        failedList.push({ symbol, reason: 'Error occurred' });
      }
    }

    setSuccessCount(successCounter);
    setFailedSymbols(failedList);
    setProcessing(false);
    
    if (successCounter > 0) {
      setNotification({
        open: true,
        message: `Successfully added ${successCounter} symbol${successCounter > 1 ? 's' : ''} to your watchlist`,
        severity: 'success'
      });
      setBulkInput(''); // Clear input on success
    } else {
      setNotification({
        open: true,
        message: 'No symbols were added to your watchlist',
        severity: 'warning'
      });
    }
  };

  // Handle individual symbol removal
  const handleRemoveSymbol = async (entityType, symbol) => {
    try {
      const success = await removeFromWatchList(entityType, symbol);
      if (success) {
        setNotification({
          open: true,
          message: `Removed ${symbol} from your watchlist`,
          severity: 'success'
        });
      } else {
        setNotification({
          open: true,
          message: `Failed to remove ${symbol}`,
          severity: 'error'
        });
      }
    } catch (error) {
      console.error(`Error removing ${symbol}:`, error);
      setNotification({
        open: true,
        message: `Error removing ${symbol}`,
        severity: 'error'
      });
    }
  };

  // Get entity type label
  const getEntityTypeLabel = (entityType) => {
    switch (entityType) {
      case 'us_stock': 
        return 'US Stock';
      case 'in_stock':
        return 'Indian Stock';
      case 'crypto_usd':
        return 'Crypto (USD)';
      case 'crypto_eth':
        return 'Crypto (ETH)';
      case 'crypto_btc':
        return 'Crypto (BTC)';
      default:
        return entityType;
    }
  };

  return (
    <>
      <Dialog 
        open={open} 
        onClose={onClose}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>
          <Box display="flex" justifyContent="space-between" alignItems="center">
            Manage Watchlist
            <IconButton onClick={onClose}>
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Box sx={{ width: '100%' }}>
            <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
              <Tabs 
                value={tabValue} 
                onChange={handleTabChange}
                variant="scrollable"
                scrollButtons="auto"
              >
                <Tab label="US Stocks" />
                <Tab label="Indian Stocks" />
                <Tab label="Crypto (USD)" />
                <Tab label="Crypto (ETH)" />
                <Tab label="Crypto (BTC)" />
              </Tabs>
            </Box>

            {/* US Stocks Tab */}
            <TabPanel value={tabValue} index={0}>
              <Box sx={{ mb: 3 }}>
                <Typography variant="h6" gutterBottom>
                  Add US Stocks
                </Typography>
                <TextField
                  fullWidth
                  multiline
                  rows={4}
                  placeholder="Enter stock symbols separated by commas, spaces, or new lines (e.g., AAPL, MSFT, GOOGL)"
                  value={bulkInput}
                  onChange={(e) => setBulkInput(e.target.value)}
                  variant="outlined"
                  sx={{ mb: 2 }}
                  disabled={processing}
                />
                <Box display="flex" justifyContent="space-between">
                  <Button 
                    variant="contained" 
                    onClick={handleBulkAdd}
                    disabled={processing || !bulkInput.trim()}
                  >
                    {processing ? <CircularProgress size={24} /> : "Add to Watchlist"}
                  </Button>
                  {successCount > 0 && (
                    <Typography variant="body2" color="success.main">
                      Added {successCount} symbol(s) successfully
                    </Typography>
                  )}
                </Box>
                {failedSymbols.length > 0 && (
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="body2" color="error">
                      Failed to add these symbols:
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 1 }}>
                      {failedSymbols.map((item, index) => (
                        <Chip 
                          key={index} 
                          label={`${item.symbol} (${item.reason})`} 
                          color="error" 
                          variant="outlined" 
                          size="small" 
                        />
                      ))}
                    </Box>
                  </Box>
                )}
              </Box>
              
              <Divider sx={{ my: 3 }} />
              
              <Typography variant="h6" gutterBottom>
                Current US Stocks Watchlist ({groupedWatchlist.us_stock.length})
              </Typography>
              <Paper variant="outlined" sx={{ maxHeight: 300, overflow: 'auto' }}>
                <List dense>
                  {groupedWatchlist.us_stock.length > 0 ? (
                    groupedWatchlist.us_stock.map((item) => (
                      <ListItem
                        key={item.symbol}
                        secondaryAction={
                          <IconButton 
                            edge="end" 
                            onClick={() => handleRemoveSymbol(item.entity_type, item.symbol)}
                            size="small"
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        }
                      >
                        <ListItemText 
                          primary={item.symbol} 
                          secondary={new Date(item.added_at).toLocaleDateString()} 
                        />
                      </ListItem>
                    ))
                  ) : (
                    <ListItem>
                      <ListItemText primary="No stocks in watchlist" />
                    </ListItem>
                  )}
                </List>
              </Paper>
            </TabPanel>

            {/* Indian Stocks Tab */}
            <TabPanel value={tabValue} index={1}>
              <Box sx={{ mb: 3 }}>
                <Typography variant="h6" gutterBottom>
                  Add Indian Stocks
                </Typography>
                <TextField
                  fullWidth
                  multiline
                  rows={4}
                  placeholder="Enter stock symbols separated by commas, spaces, or new lines (e.g., RELIANCE, INFY, TCS)"
                  value={bulkInput}
                  onChange={(e) => setBulkInput(e.target.value)}
                  variant="outlined"
                  sx={{ mb: 2 }}
                  disabled={processing}
                />
                <Box display="flex" justifyContent="space-between">
                  <Button 
                    variant="contained" 
                    onClick={handleBulkAdd}
                    disabled={processing || !bulkInput.trim()}
                  >
                    {processing ? <CircularProgress size={24} /> : "Add to Watchlist"}
                  </Button>
                  {successCount > 0 && (
                    <Typography variant="body2" color="success.main">
                      Added {successCount} symbol(s) successfully
                    </Typography>
                  )}
                </Box>
                {failedSymbols.length > 0 && (
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="body2" color="error">
                      Failed to add these symbols:
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 1 }}>
                      {failedSymbols.map((item, index) => (
                        <Chip 
                          key={index} 
                          label={`${item.symbol} (${item.reason})`} 
                          color="error" 
                          variant="outlined" 
                          size="small" 
                        />
                      ))}
                    </Box>
                  </Box>
                )}
              </Box>
              
              <Divider sx={{ my: 3 }} />
              
              <Typography variant="h6" gutterBottom>
                Current Indian Stocks Watchlist ({groupedWatchlist.in_stock.length})
              </Typography>
              <Paper variant="outlined" sx={{ maxHeight: 300, overflow: 'auto' }}>
                <List dense>
                  {groupedWatchlist.in_stock.length > 0 ? (
                    groupedWatchlist.in_stock.map((item) => (
                      <ListItem
                        key={item.symbol}
                        secondaryAction={
                          <IconButton 
                            edge="end" 
                            onClick={() => handleRemoveSymbol(item.entity_type, item.symbol)}
                            size="small"
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        }
                      >
                        <ListItemText 
                          primary={item.symbol} 
                          secondary={new Date(item.added_at).toLocaleDateString()} 
                        />
                      </ListItem>
                    ))
                  ) : (
                    <ListItem>
                      <ListItemText primary="No stocks in watchlist" />
                    </ListItem>
                  )}
                </List>
              </Paper>
            </TabPanel>

            {/* Crypto (USD) Tab */}
            <TabPanel value={tabValue} index={2}>
              <Box sx={{ mb: 3 }}>
                <Typography variant="h6" gutterBottom>
                  Add Cryptocurrencies (USD)
                </Typography>
                <TextField
                  fullWidth
                  multiline
                  rows={4}
                  placeholder="Enter crypto symbols separated by commas, spaces, or new lines (e.g., BTCUSD, ETHUSD, SOLUSD)"
                  value={bulkInput}
                  onChange={(e) => setBulkInput(e.target.value)}
                  variant="outlined"
                  sx={{ mb: 2 }}
                  disabled={processing}
                />
                <Box display="flex" justifyContent="space-between">
                  <Button 
                    variant="contained" 
                    onClick={handleBulkAdd}
                    disabled={processing || !bulkInput.trim()}
                  >
                    {processing ? <CircularProgress size={24} /> : "Add to Watchlist"}
                  </Button>
                  {successCount > 0 && (
                    <Typography variant="body2" color="success.main">
                      Added {successCount} symbol(s) successfully
                    </Typography>
                  )}
                </Box>
                {failedSymbols.length > 0 && (
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="body2" color="error">
                      Failed to add these symbols:
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 1 }}>
                      {failedSymbols.map((item, index) => (
                        <Chip 
                          key={index} 
                          label={`${item.symbol} (${item.reason})`} 
                          color="error" 
                          variant="outlined" 
                          size="small" 
                        />
                      ))}
                    </Box>
                  </Box>
                )}
              </Box>
              
              <Divider sx={{ my: 3 }} />
              
              <Typography variant="h6" gutterBottom>
                Current Crypto (USD) Watchlist ({groupedWatchlist.crypto_usd.length})
              </Typography>
              <Paper variant="outlined" sx={{ maxHeight: 300, overflow: 'auto' }}>
                <List dense>
                  {groupedWatchlist.crypto_usd.length > 0 ? (
                    groupedWatchlist.crypto_usd.map((item) => (
                      <ListItem
                        key={item.symbol}
                        secondaryAction={
                          <IconButton 
                            edge="end" 
                            onClick={() => handleRemoveSymbol(item.entity_type, item.symbol)}
                            size="small"
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        }
                      >
                        <ListItemText 
                          primary={item.symbol} 
                          secondary={new Date(item.added_at).toLocaleDateString()} 
                        />
                      </ListItem>
                    ))
                  ) : (
                    <ListItem>
                      <ListItemText primary="No cryptocurrencies in watchlist" />
                    </ListItem>
                  )}
                </List>
              </Paper>
            </TabPanel>

            {/* Crypto (ETH) Tab */}
            <TabPanel value={tabValue} index={3}>
              <Box sx={{ mb: 3 }}>
                <Typography variant="h6" gutterBottom>
                  Add Cryptocurrencies (ETH Base)
                </Typography>
                <TextField
                  fullWidth
                  multiline
                  rows={4}
                  placeholder="Enter crypto symbols separated by commas, spaces, or new lines"
                  value={bulkInput}
                  onChange={(e) => setBulkInput(e.target.value)}
                  variant="outlined"
                  sx={{ mb: 2 }}
                  disabled={processing}
                />
                <Box display="flex" justifyContent="space-between">
                  <Button 
                    variant="contained" 
                    onClick={handleBulkAdd}
                    disabled={processing || !bulkInput.trim()}
                  >
                    {processing ? <CircularProgress size={24} /> : "Add to Watchlist"}
                  </Button>
                  {successCount > 0 && (
                    <Typography variant="body2" color="success.main">
                      Added {successCount} symbol(s) successfully
                    </Typography>
                  )}
                </Box>
                {failedSymbols.length > 0 && (
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="body2" color="error">
                      Failed to add these symbols:
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 1 }}>
                      {failedSymbols.map((item, index) => (
                        <Chip 
                          key={index} 
                          label={`${item.symbol} (${item.reason})`} 
                          color="error" 
                          variant="outlined" 
                          size="small" 
                        />
                      ))}
                    </Box>
                  </Box>
                )}
              </Box>
              
              <Divider sx={{ my: 3 }} />
              
              <Typography variant="h6" gutterBottom>
                Current Crypto (ETH) Watchlist ({groupedWatchlist.crypto_eth.length})
              </Typography>
              <Paper variant="outlined" sx={{ maxHeight: 300, overflow: 'auto' }}>
                <List dense>
                  {groupedWatchlist.crypto_eth.length > 0 ? (
                    groupedWatchlist.crypto_eth.map((item) => (
                      <ListItem
                        key={item.symbol}
                        secondaryAction={
                          <IconButton 
                            edge="end" 
                            onClick={() => handleRemoveSymbol(item.entity_type, item.symbol)}
                            size="small"
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        }
                      >
                        <ListItemText 
                          primary={item.symbol} 
                          secondary={new Date(item.added_at).toLocaleDateString()} 
                        />
                      </ListItem>
                    ))
                  ) : (
                    <ListItem>
                      <ListItemText primary="No cryptocurrencies in watchlist" />
                    </ListItem>
                  )}
                </List>
              </Paper>
            </TabPanel>

            {/* Crypto (BTC) Tab */}
            <TabPanel value={tabValue} index={4}>
              <Box sx={{ mb: 3 }}>
                <Typography variant="h6" gutterBottom>
                  Add Cryptocurrencies (BTC Base)
                </Typography>
                <TextField
                  fullWidth
                  multiline
                  rows={4}
                  placeholder="Enter crypto symbols separated by commas, spaces, or new lines"
                  value={bulkInput}
                  onChange={(e) => setBulkInput(e.target.value)}
                  variant="outlined"
                  sx={{ mb: 2 }}
                  disabled={processing}
                />
                <Box display="flex" justifyContent="space-between">
                  <Button 
                    variant="contained" 
                    onClick={handleBulkAdd}
                    disabled={processing || !bulkInput.trim()}
                  >
                    {processing ? <CircularProgress size={24} /> : "Add to Watchlist"}
                  </Button>
                  {successCount > 0 && (
                    <Typography variant="body2" color="success.main">
                      Added {successCount} symbol(s) successfully
                    </Typography>
                  )}
                </Box>
                {failedSymbols.length > 0 && (
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="body2" color="error">
                      Failed to add these symbols:
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 1 }}>
                      {failedSymbols.map((item, index) => (
                        <Chip 
                          key={index} 
                          label={`${item.symbol} (${item.reason})`} 
                          color="error" 
                          variant="outlined" 
                          size="small" 
                        />
                      ))}
                    </Box>
                  </Box>
                )}
              </Box>
              
              <Divider sx={{ my: 3 }} />
              
              <Typography variant="h6" gutterBottom>
                Current Crypto (BTC) Watchlist ({groupedWatchlist.crypto_btc.length})
              </Typography>
              <Paper variant="outlined" sx={{ maxHeight: 300, overflow: 'auto' }}>
                <List dense>
                  {groupedWatchlist.crypto_btc.length > 0 ? (
                    groupedWatchlist.crypto_btc.map((item) => (
                      <ListItem
                        key={item.symbol}
                        secondaryAction={
                          <IconButton 
                            edge="end" 
                            onClick={() => handleRemoveSymbol(item.entity_type, item.symbol)}
                            size="small"
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        }
                      >
                        <ListItemText 
                          primary={item.symbol} 
                          secondary={new Date(item.added_at).toLocaleDateString()} 
                        />
                      </ListItem>
                    ))
                  ) : (
                    <ListItem>
                      <ListItemText primary="No cryptocurrencies in watchlist" />
                    </ListItem>
                  )}
                </List>
              </Paper>
            </TabPanel>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={onClose} color="primary">
            Close
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={() => setNotification({ ...notification, open: false })}
      >
        <Alert 
          onClose={() => setNotification({ ...notification, open: false })} 
          severity={notification.severity} 
          sx={{ width: '100%' }}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </>
  );
}

export default ManageWatchlistModal;