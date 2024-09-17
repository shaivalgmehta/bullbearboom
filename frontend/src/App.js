import React, { useState, useEffect } from 'react';
import axios from 'axios';

// Use an environment variable for the API URL
const API_URL = process.env.REACT_APP_API_URL || '/api';

function App() {
  const [stockData, setStockData] = useState([]);
  const [symbol, setSymbol] = useState('AAPL');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const result = await axios.get(`${API_URL}/stock/${symbol}`);
        setStockData(result.data);
      } catch (error) {
        console.error("Error fetching stock data:", error);
        // Handle the error appropriately
      }
    };

    fetchData();
  }, [symbol]);

  return (
    <div className="App">
      <h1>Stock Data for {symbol}</h1>
      <input 
        type="text" 
        value={symbol} 
        onChange={(e) => setSymbol(e.target.value.toUpperCase())}
        placeholder="Enter stock symbol"
      />
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Price</th>
            <th>Volume</th>
          </tr>
        </thead>
        <tbody>
          {stockData.map((data, index) => (
            <tr key={index}>
              <td>{new Date(data.time).toLocaleString()}</td>
              <td>{data.price}</td>
              <td>{data.volume}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;