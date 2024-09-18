from abc import ABC, abstractmethod
from typing import Dict, Any, List

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

class TwelveDataScreenerTransformer(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        stock_data = data['stock_data']
        statistics = data['statistics']
        
        transformed_data = {
            'stock': stock_data['symbol'],
            'market_cap': self._parse_numeric(statistics['statistics']['valuations_metrics']['market_capitalization']),
            'pe_ratio': self._parse_numeric(statistics['statistics']['valuations_metrics']['trailing_pe']),
            'ev_ebitda': self._parse_numeric(statistics['statistics']['valuations_metrics']['enterprise_to_ebitda']),
            'pb_ratio': self._parse_numeric(statistics['statistics']['valuations_metrics']['price_to_book_mrq']),
            'peg_ratio': self._parse_numeric(statistics['statistics']['valuations_metrics']['peg_ratio']),
            'current_year_sales': self._parse_numeric(statistics['statistics']['financials']['income_statement']['revenue_ttm']),
            'current_year_ebitda': self._parse_numeric(statistics['statistics']['financials']['income_statement']['ebitda'])
        }
        return [transformed_data]

    def _parse_numeric(self, value: Any) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, str):
            try:
                return float(value.replace(',', ''))
            except ValueError:
                return None
        else:
            return None

def get_transformer(source: str) -> BaseTransformer:
    if source.lower() == 'twelvedata_screener':
        return TwelveDataScreenerTransformer()
    else:
        raise ValueError(f"Unsupported data source: {source}")