from abc import ABC, abstractmethod
from typing import Dict, Any, List

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

class TwelveDataTransformer(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        transformed_data = []
        for item in data.get('values', []):
            transformed_item = {
                'timestamp': item['datetime'],
                'symbol': data['meta']['symbol'],
                'open': float(item['open']),
                'high': float(item['high']),
                'low': float(item['low']),
                'close': float(item['close']),
                'volume': int(item['volume'])
            }
            # You can add more calculated fields here
            transformed_data.append(transformed_item)
        return transformed_data

def get_transformer(source: str) -> BaseTransformer:
    if source.lower() == 'twelvedata':
        return TwelveDataTransformer()
    else:
        raise ValueError(f"Unsupported data source: {source}")