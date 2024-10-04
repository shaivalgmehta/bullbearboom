import React, { useState } from 'react';
import { BrowserRouter as Router, Route, Link, Routes } from 'react-router-dom';
import { AppBar, Toolbar, Typography, Button, Container, IconButton } from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import StockApp from './StockApp';
import CryptoApp from './CryptoApp';

function MainApp() {
  const [drawerOpen, setDrawerOpen] = useState(false);

  const toggleDrawer = () => {
    setDrawerOpen(!drawerOpen);
  };

  return (
    <Router>
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
          <Button color="inherit" component={Link} to="/crypto">
            Crypto
          </Button>
        </Toolbar>
      </AppBar>

      <Container sx={{ mt: 8 }}>
        <Routes>
          <Route path="/" element={<StockApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
          <Route path="/crypto" element={<CryptoApp drawerOpen={drawerOpen} toggleDrawer={toggleDrawer} />} />
        </Routes>
      </Container>
    </Router>
  );
}

export default MainApp;