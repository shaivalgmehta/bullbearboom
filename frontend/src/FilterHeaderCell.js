function FilterHeaderCell({
  column,
  columnName,
  sortConfig,
  filters,
  pendingFilters,
  onRequestSort,
  onFilterChange,
  onClearFilter,
  isNumeric = false,
  isTextFilter = false
}) {
  const handleMinChange = (e) => {
    const value = e.target.value === '' ? undefined : parseFloat(e.target.value);
    onFilterChange(column, value, 'min');
  };

  const handleMaxChange = (e) => {
    const value = e.target.value === '' ? undefined : parseFloat(e.target.value);
    onFilterChange(column, value, 'max');
  };

  const handleTextChange = (e) => {
    onFilterChange(column, e.target.value);
  };

  const currentFilters = pendingFilters[column] || {};
  const hasActiveFilter = isNumeric 
    ? (currentFilters.min !== undefined || currentFilters.max !== undefined)
    : (currentFilters && currentFilters !== '');

  return (
    <TableCell 
      align={isNumeric ? "center" : "left"}
      sx={{ 
        padding: '4px 8px',
        verticalAlign: 'top',
        backgroundColor: '#f8f9fa'
      }}
    >
      <Box sx={{ 
        display: 'flex', 
        flexDirection: 'column',
        gap: 1
      }}>
        {/* Header with column name */}
        <Box sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: isNumeric ? "center" : "flex-start",
          minHeight: '32px'
        }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 'bold' }}>
            {columnName}
          </Typography>
          {hasActiveFilter && (
            <Tooltip title="Clear filter">
              <Button 
                size="small" 
                onClick={() => onClearFilter(column)}
                sx={{ minWidth: 'auto', p: 0.5, ml: 1 }}
              >
                ×
              </Button>
            </Tooltip>
          )}
        </Box>

        {/* Filter inputs */}
        {isTextFilter && (
          <TextField
            size="small"
            placeholder={`Filter ${columnName}`}
            value={pendingFilters[column] || ''}
            onChange={handleTextChange}
            variant="outlined"
            sx={{
              '& .MuiInputBase-input': {
                padding: '4px 8px',
                fontSize: '0.75rem'
              }
            }}
          />
        )}

        {isNumeric && (
          <Box sx={{ 
            display: 'flex', 
            gap: 0.5,
            alignItems: 'center',
            justifyContent: 'center'
          }}>
            <TextField
              size="small"
              placeholder="Min"
              value={currentFilters.min || ''}
              onChange={handleMinChange}
              variant="outlined"
              sx={{
                width: '70px',
                '& .MuiInputBase-input': {
                  padding: '4px 8px',
                  fontSize: '0.75rem'
                }
              }}
            />
            <TextField
              size="small"
              placeholder="Max"
              value={currentFilters.max || ''}
              onChange={handleMaxChange}
              variant="outlined"
              sx={{
                width: '70px',
                '& .MuiInputBase-input': {
                  padding: '4px 8px',
                  fontSize: '0.75rem'
                }
              }}
            />
          </Box>
        )}
      </Box>
    </TableCell>
  );
}