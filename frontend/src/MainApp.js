import React, { useState } from 'react';
import { BrowserRouter as Router, Route, Link, Routes } from 'react-router-dom';
import { AppBar, Toolbar, Typography, Button, Box, IconButton, Menu, MenuItem } from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import StockApp from './StockApp';
import StockAlertsApp from './StockAlertsApp';
import CryptoApp from './CryptoApp';
import CryptoETHApp from './CryptoETHApp';
import CryptoBTCApp from './CryptoBTCApp';
import CryptoAlertsApp from './CryptoAlertsApp';
import StockDetailApp from './StockDetailApp/StockDetailApp';
import CryptoDetailApp from './CryptoDetailApp/CryptoDetailApp';

function MainApp() {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [cryptoMenuAnchor, setCryptoMenuAnchor] = useState(null);
  const [alertsMenuAnchor, setAlertsMenuAnchor] = useState(null);

  const toggleDrawer = () => {
    setDrawerOpen(!drawerOpen);
  };

  const handleCryptoMenuOpen = (event) => {
    setCryptoMenuAnchor(event.currentTarget);
  };

  const handleCryptoMenuClose = () => {
    setCryptoMenuAnchor(null);
  };

  const handleAlertsMenuOpen = (event) => {
    setAlertsMenuAnchor(event.currentTarget);
  };

  const handleAlertsMenuClose = () => {
    setAlertsMenuAnchor(null);
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
            <Typography variant="h6" style={{ flexGrow: 1 }}>
              Bull Bear Boom
            </Typography>

            {/* Alerts Menu */}
            <Box>
              <Button 
                color="inherit"
                onClick={handleAlertsMenuOpen}
                endIcon={<ArrowDropDownIcon />}
              >
                Alerts
              </Button>
              <Menu
                anchorEl={alertsMenuAnchor}
                open={Boolean(alertsMenuAnchor)}
                onClose={handleAlertsMenuClose}
              >
                <MenuItem 
                  component={Link} 
                  to="/"
                  onClick={handleAlertsMenuClose}
                >
                  Stock Alerts
                </MenuItem>
                <MenuItem 
                  component={Link} 
                  to="/crypto_alerts"
                  onClick={handleAlertsMenuClose}
                >
                  Crypto Alerts
                </MenuItem>
              </Menu>
            </Box>
            
            {/* Stocks Menu */}
            <Button color="inherit" component={Link} to="/us_stocks">
              Stocks
            </Button>
            
            {/* Crypto Menu */}
            <Box>
              <Button 
                color="inherit"
                onClick={handleCryptoMenuOpen}
                endIcon={<ArrowDropDownIcon />}
              >
                Crypto
              </Button>
              <Menu
                anchorEl={cryptoMenuAnchor}
                open={Boolean(cryptoMenuAnchor)}
                onClose={handleCryptoMenuClose}
              >
                <MenuItem 
                  component={Link} 
                  to="/crypto"
                  onClick={handleCryptoMenuClose}
                >
                  USD Base
                </MenuItem>
                <MenuItem 
                  component={Link} 
                  to="/crypto_eth"
                  onClick={handleCryptoMenuClose}
                >
                  ETH Base
                </MenuItem>
                <MenuItem 
                  component={Link} 
                  to="/crypto_btc"
                  onClick={handleCryptoMenuClose}
                >
                  BTC Base
                </MenuItem>
              </Menu>
            </Box>
          </Toolbar>
        </AppBar>

        <Box sx={{ flexGrow: 1, mt: '64px' }}>
          <Routes>
            <Route path="/" element={<StockAlertsApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="us_stocks" element={<StockApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="stock/:symbol" element={<StockDetailApp />} />
            <Route path="crypto_alerts" element={<CryptoAlertsApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="crypto" element={<CryptoApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="crypto_eth" element={<CryptoETHApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="crypto_btc" element={<CryptoBTCApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="crypto/:symbol" element={<CryptoDetailApp />} />
            <Route path="crypto_eth/:symbol" element={<CryptoDetailApp />} />
            <Route path="crypto_btc/:symbol" element={<CryptoDetailApp />} />
          </Routes>
        </Box>
      </Box>
    </Router>
  );
}

export default MainApp;