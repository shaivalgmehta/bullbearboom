import React, { useState } from 'react';
import { BrowserRouter as Router, Route, Link, Routes } from 'react-router-dom';
import { 
  AppBar, 
  Toolbar, 
  Typography, 
  Button, 
  Box, 
  IconButton, 
  Menu, 
  MenuItem,
  ListItemIcon,
  ListItemText,
  Divider
} from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import ShowChartIcon from '@mui/icons-material/ShowChart';
import NotificationsIcon from '@mui/icons-material/Notifications';
import CurrencyBitcoinIcon from '@mui/icons-material/CurrencyBitcoin';
import CurrencyRupeeIcon from '@mui/icons-material/CurrencyRupee';
import AttachMoneyIcon from '@mui/icons-material/AttachMoney';
import TimelineIcon from '@mui/icons-material/Timeline';
import TrendingUpIcon from '@mui/icons-material/TrendingUp';
import BusinessCenterIcon from '@mui/icons-material/BusinessCenter';
import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';

// Import all your route components
import US_StockApp from './US_StockApp';
import US_StockAlertsApp from './US_StockAlertsApp';
import US_StockDetailApp from './US_StockDetailApp/US_StockDetailApp';
import US_InsiderTradingApp from './US_InsiderTradingApp';
import IN_StockApp from './IN_StockApp';
import IN_StockAlertsApp from './IN_StockAlertsApp';
import IN_StockDetailApp from './IN_StockDetailApp/IN_StockDetailApp';
import CryptoApp from './CryptoApp';
import CryptoETHApp from './CryptoETHApp';
import CryptoBTCApp from './CryptoBTCApp';
import CryptoAlertsApp from './CryptoAlertsApp';
import CryptoDetailApp from './CryptoDetailApp/CryptoDetailApp';
import CryptoFibonacciApp from './CryptoFibonacciApp';

function MainApp() {
  // Menu anchor states
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [alertsAnchor, setAlertsAnchor] = useState(null);
  const [stocksAnchor, setStocksAnchor] = useState(null);
  const [cryptoAnchor, setCryptoAnchor] = useState(null);
  const [usStocksAnchor, setUsStocksAnchor] = useState(null);
  const [inStocksAnchor, setInStocksAnchor] = useState(null);

  const toggleDrawer = () => {
    setDrawerOpen(!drawerOpen);
  };

  // Handle menu openings
  const handleMenuOpen = (event, setAnchor) => {
    setAnchor(event.currentTarget);
  };

  // Handle menu closings
  const handleMenuClose = (setAnchor) => {
    setAnchor(null);
  };

  // Close all submenus
  const closeAllMenus = () => {
    setAlertsAnchor(null);
    setStocksAnchor(null);
    setCryptoAnchor(null);
    setUsStocksAnchor(null);
    setInStocksAnchor(null);
  };

  return (
    <Router>
      <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
        <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
          <Toolbar>
            <IconButton
              color="inherit"
              aria-label="open drawer"
              edge="start"
              onClick={toggleDrawer}
              sx={{ mr: 2 }}
            >
              <MenuIcon />
            </IconButton>
            
            <Typography variant="h6" sx={{ flexGrow: 1 }}>
              Bull Bear Boom
            </Typography>

            {/* Alerts Menu */}
            <Button
              color="inherit"
              startIcon={<NotificationsIcon />}
              endIcon={<ArrowDropDownIcon />}
              onClick={(e) => handleMenuOpen(e, setAlertsAnchor)}
            >
              Alerts
            </Button>
            <Menu
              anchorEl={alertsAnchor}
              open={Boolean(alertsAnchor)}
              onClose={() => handleMenuClose(setAlertsAnchor)}
            >
              <MenuItem component={Link} to="/" onClick={closeAllMenus}>
                <ListItemIcon>
                  <AttachMoneyIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>US Stock Alerts</ListItemText>
              </MenuItem>
              <MenuItem component={Link} to="/in_alerts" onClick={closeAllMenus}>
                <ListItemIcon>
                  <CurrencyRupeeIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>India Stock Alerts</ListItemText>
              </MenuItem>
              <MenuItem component={Link} to="/crypto_alerts" onClick={closeAllMenus}>
                <ListItemIcon>
                  <CurrencyBitcoinIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>Crypto Alerts</ListItemText>
              </MenuItem>
            </Menu>

            {/* Stocks Menu */}
            <Button
              color="inherit"
              startIcon={<ShowChartIcon />}
              endIcon={<ArrowDropDownIcon />}
              onClick={(e) => handleMenuOpen(e, setStocksAnchor)}
            >
              Stocks
            </Button>
            <Menu
              anchorEl={stocksAnchor}
              open={Boolean(stocksAnchor)}
              onClose={() => handleMenuClose(setStocksAnchor)}
            >
              {/* US Markets Submenu */}
              <MenuItem 
                onClick={(e) => {
                  e.stopPropagation();
                  handleMenuOpen(e, setUsStocksAnchor);
                }}
              >
                <ListItemIcon>
                  <AttachMoneyIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>US Markets</ListItemText>
                <ArrowDropDownIcon />
              </MenuItem>
              
              {/* India Markets Submenu */}
              <MenuItem 
                onClick={(e) => {
                  e.stopPropagation();
                  handleMenuOpen(e, setInStocksAnchor);
                }}
              >
                <ListItemIcon>
                  <CurrencyRupeeIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>India Markets</ListItemText>
                <ArrowDropDownIcon />
              </MenuItem>
            </Menu>

            {/* US Stocks Submenu */}
            <Menu
              anchorEl={usStocksAnchor}
              open={Boolean(usStocksAnchor)}
              onClose={() => handleMenuClose(setUsStocksAnchor)}
              anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
              transformOrigin={{ vertical: 'top', horizontal: 'left' }}
            >
              <MenuItem component={Link} to="/us_stocks" onClick={closeAllMenus}>
                <ListItemIcon>
                  <TimelineIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>Stock Screener</ListItemText>
              </MenuItem>
              <MenuItem component={Link} to="/us_insider_trading" onClick={closeAllMenus}>
                <ListItemIcon>
                  <BusinessCenterIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>Insider Trading</ListItemText>
              </MenuItem>
            </Menu>

            {/* India Stocks Submenu */}
            <Menu
              anchorEl={inStocksAnchor}
              open={Boolean(inStocksAnchor)}
              onClose={() => handleMenuClose(setInStocksAnchor)}
              anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
              transformOrigin={{ vertical: 'top', horizontal: 'left' }}
            >
              <MenuItem component={Link} to="/in_stocks" onClick={closeAllMenus}>
                <ListItemIcon>
                  <TimelineIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>Stock Screener</ListItemText>
              </MenuItem>
            </Menu>

            {/* Crypto Menu */}
            <Button
              color="inherit"
              startIcon={<CurrencyBitcoinIcon />}
              endIcon={<ArrowDropDownIcon />}
              onClick={(e) => handleMenuOpen(e, setCryptoAnchor)}
            >
              Crypto
            </Button>
            <Menu
              anchorEl={cryptoAnchor}
              open={Boolean(cryptoAnchor)}
              onClose={() => handleMenuClose(setCryptoAnchor)}
            >
              <MenuItem component={Link} to="/crypto" onClick={closeAllMenus}>
                <ListItemIcon>
                  <AttachMoneyIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>USD Base</ListItemText>
              </MenuItem>
              <MenuItem component={Link} to="/crypto_eth" onClick={closeAllMenus}>
                <ListItemIcon>
                  <CurrencyBitcoinIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>ETH Base</ListItemText>
              </MenuItem>
              <MenuItem component={Link} to="/crypto_btc" onClick={closeAllMenus}>
                <ListItemIcon>
                  <CurrencyBitcoinIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>BTC Base</ListItemText>
              </MenuItem>
              <Divider />
              <MenuItem component={Link} to="/crypto/fibonacci" onClick={closeAllMenus}>
                <ListItemIcon>
                  <TrendingUpIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText>Fibonacci Analysis</ListItemText>
              </MenuItem>
            </Menu>
          </Toolbar>
        </AppBar>

        {/* Routes */}
        <Box sx={{ flexGrow: 1, mt: '64px' }}>
          <Routes>
            <Route path="/" element={<US_StockAlertsApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="us_stocks" element={<US_StockApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="us_stock/:symbol" element={<US_StockDetailApp />} />
            <Route path="in_alerts" element={<IN_StockAlertsApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="in_stocks" element={<IN_StockApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="in_stock/:symbol" element={<IN_StockDetailApp />} />            
            <Route path="crypto_alerts" element={<CryptoAlertsApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="crypto" element={<CryptoApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="crypto_eth" element={<CryptoETHApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="crypto_btc" element={<CryptoBTCApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="crypto/:symbol" element={<CryptoDetailApp />} />
            <Route path="crypto_eth/:symbol" element={<CryptoDetailApp />} />
            <Route path="crypto_btc/:symbol" element={<CryptoDetailApp />} />
            <Route path="us_insider_trading" element={<US_InsiderTradingApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="crypto/fibonacci" element={<CryptoFibonacciApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />          
          </Routes>
        </Box>
      </Box>
    </Router>
  );
}

export default MainApp;