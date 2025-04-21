import React, { useState, useContext } from 'react';
import { 
  Button, 
  Menu, 
  MenuItem, 
  IconButton, 
  Typography, 
  Box,
  Divider,
  ListItemIcon
} from '@mui/material';
import AccountCircleIcon from '@mui/icons-material/AccountCircle';
import BookmarksIcon from '@mui/icons-material/Bookmarks';
import LogoutIcon from '@mui/icons-material/Logout';
import { AuthContext } from './AuthContext';
import AuthModal from './AuthModal';
import ManageWatchlistModal from '../Watchlist/ManageWatchlistModal';

function UserMenu() {
  const { user, logout } = useContext(AuthContext);
  const [anchorEl, setAnchorEl] = useState(null);
  const [authModalOpen, setAuthModalOpen] = useState(false);
  const [watchlistModalOpen, setWatchlistModalOpen] = useState(false);
  
  const handleClick = (event) => {
    setAnchorEl(event.currentTarget);
  };
  
  const handleClose = () => {
    setAnchorEl(null);
  };
  
  const handleLogout = () => {
    logout();
    handleClose();
  };

  const handleWatchlistOpen = () => {
    handleClose();
    setWatchlistModalOpen(true);
  };
  
  return (
    <>
      {user ? (
        <>
          <IconButton
            color="inherit"
            onClick={handleClick}
          >
            <AccountCircleIcon />
          </IconButton>
          <Menu
            anchorEl={anchorEl}
            open={Boolean(anchorEl)}
            onClose={handleClose}
          >
            <Box sx={{ px: 2, py: 1 }}>
              <Typography variant="subtitle2">{user.email}</Typography>
            </Box>
            <MenuItem onClick={handleWatchlistOpen}>
              <ListItemIcon>
                <BookmarksIcon fontSize="small" />
              </ListItemIcon>
              Manage Watchlist
            </MenuItem>
            <Divider />
            <MenuItem onClick={handleLogout}>
              <ListItemIcon>
                <LogoutIcon fontSize="small" />
              </ListItemIcon>
              Logout
            </MenuItem>
          </Menu>
        </>
      ) : (
        <Button 
          color="inherit" 
          onClick={() => setAuthModalOpen(true)}
        >
          Login
        </Button>
      )}
      
      <AuthModal
        open={authModalOpen}
        onClose={() => setAuthModalOpen(false)}
      />
      
      {user && (
        <ManageWatchlistModal
          open={watchlistModalOpen}
          onClose={() => setWatchlistModalOpen(false)}
        />
      )}
    </>
  );
}

export default UserMenu;