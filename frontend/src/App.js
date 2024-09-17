import React, { useState, useEffect } from 'react';
import axios from 'axios';

function App() {
  const [stockData, setStockData] = useState([]);
  const [symbol, setSymbol] = useState('AAPL');

  useEffect(() => {
    const fetchData = async () => {
      const result = await axios.get(`http://localhost:5000/api/stock/${symbol}`);
      setStockData(result.data);
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