import React, { createContext, useState, useEffect, useContext } from 'react';
import axios from 'axios';
import { AuthContext } from '../Authentication/AuthContext';


const API_URL = process.env.REACT_APP_API_URL || '/api';


export const WatchListContext = createContext();

export const WatchListProvider = ({ children }) => {
  const [watchList, setWatchList] = useState([]);
  const [loading, setLoading] = useState(false);
  const { user } = useContext(AuthContext);
  
  useEffect(() => {
    if (user) {
      fetchWatchList();
    } else {
      setWatchList([]);
    }
  }, [user]);
  
  const fetchWatchList = async () => {
    if (!user) return;
    
    setLoading(true);
    try {
      const response = await axios.get(`${API_URL}/watchlist`);
      setWatchList(response.data);
    } catch (error) {
      console.error('Error fetching watch list:', error);
    } finally {
      setLoading(false);
    }
  };
  
  const isInWatchList = (entityType, symbol) => {
    return watchList.some(item => 
      item.entity_type === entityType && item.symbol === symbol
    );
  };
  
  const addToWatchList = async (entityType, symbol) => {
    if (!user) return false;
    
    try {
      await axios.post(`${API_URL}/watchlist`, { entity_type: entityType, symbol });
      await fetchWatchList();
      return true;
    } catch (error) {
      console.error('Error adding to watch list:', error);
      return false;
    }
  };

  const bulkAddToWatchList = async (entityType, symbols) => {
    if (!user) return { success: [], failures: symbols.map(s => ({ symbol: s, reason: 'Not logged in' })) };
    
    const results = {
      success: [],
      failures: []
    };
    
    try {
      for (const symbol of symbols) {
        // Skip if already in watchlist
        if (isInWatchList(entityType, symbol)) {
          results.failures.push({ symbol, reason: 'Already in watchlist' });
          continue;
        }
        
        try {
          await axios.post(`${API_URL}/watchlist`, { entity_type: entityType, symbol });
          results.success.push(symbol);
        } catch (error) {
          results.failures.push({ symbol, reason: 'API error' });
        }
      }
      
      await fetchWatchList();
      return results;
    } catch (error) {
      console.error('Error in bulk add to watch list:', error);
      return { 
        success: results.success, 
        failures: [...results.failures, ...symbols
          .filter(s => !results.success.includes(s) && 
                       !results.failures.some(f => f.symbol === s))
          .map(s => ({ symbol: s, reason: 'Unknown error' }))
        ] 
      };
    }
  };
  
  const removeFromWatchList = async (entityType, symbol) => {
    if (!user) return false;
    
    try {
      await axios.delete(`${API_URL}/watchlist`, { 
        data: { entity_type: entityType, symbol } 
      });
      await fetchWatchList();
      return true;
    } catch (error) {
      console.error('Error removing from watch list:', error);
      return false;
    }
  };
  
  return (
    <WatchListContext.Provider 
      value={{ 
        watchList, 
        loading, 
        isInWatchList, 
        addToWatchList,
        bulkAddToWatchList,
        removeFromWatchList,
        fetchWatchList
      }}
    >
      {children}
    </WatchListContext.Provider>
  );
};