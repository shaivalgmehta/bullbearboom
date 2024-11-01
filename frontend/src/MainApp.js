import React, { useState } from 'react';
import { BrowserRouter as Router, Route, Link, Routes } from 'react-router-dom';
import { AppBar, Toolbar, Typography, Button, Box, IconButton } from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import StockApp from './StockApp';
import AlertsApp from './AlertsApp';
import CryptoApp from './CryptoApp';
import CryptoETHApp from './CryptoETHApp';
import CryptoBTCApp from './CryptoBTCApp';

function MainApp() {
  const [drawerOpen, setDrawerOpen] = useState(false);

  const toggleDrawer = () => {
    setDrawerOpen(!drawerOpen);
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
            <Button color="inherit" component={Link} to="/">
              Stocks
            </Button>
            <Button color="inherit" component={Link} to="/alerts">
              Alerts
            </Button>
            <Button color="inherit" component={Link} to="/crypto">
              Crypto
            </Button>
            <Button color="inherit" component={Link} to="/crypto_eth">
              Crypto ETH
            </Button>
            <Button color="inherit" component={Link} to="/crypto_btc">
              Crypto BTC
            </Button>
          </Toolbar>
        </AppBar>

        <Box sx={{ flexGrow: 1, mt: '64px' }}>
          <Routes>
            <Route path="/" element={<StockApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="/alerts" element={<AlertsApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="/crypto" element={<CryptoApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="/crypto_eth" element={<CryptoETHApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
            <Route path="/crypto_btc" element={<CryptoBTCApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
          </Routes>
        </Box>
      </Box>
    </Router>
  );
}

export default MainApp;