import React, { useContext, useState } from 'react';
import { IconButton, Tooltip } from '@mui/material';
import StarIcon from '@mui/icons-material/Star';
import StarOutlineIcon from '@mui/icons-material/StarOutline';
import { WatchListContext } from './WatchListContext';
import { AuthContext } from '../Authentication/AuthContext';

function WatchListStar({ entityType, symbol }) {
  const { user } = useContext(AuthContext);
  const { isInWatchList, addToWatchList, removeFromWatchList } = useContext(WatchListContext);
  const [loading, setLoading] = useState(false);
  
  const isWatched = isInWatchList(entityType, symbol);
  
  const handleClick = async (e) => {
    e.stopPropagation(); // Prevent triggering row click
    
    if (!user) {
      // Show login modal
      return;
    }
    
    setLoading(true);
    if (isWatched) {
      await removeFromWatchList(entityType, symbol);
    } else {
      await addToWatchList(entityType, symbol);
    }
    setLoading(false);
  };
  
  return (
    <Tooltip title={isWatched ? "Remove from Watch List" : "Add to Watch List"}>
      <IconButton onClick={handleClick} disabled={loading} size="small">
        {isWatched ? (
          <StarIcon sx={{ color: '#FFD700' }} />
        ) : (
          <StarOutlineIcon />
        )}
      </IconButton>
    </Tooltip>
  );
}

export default WatchListStar;