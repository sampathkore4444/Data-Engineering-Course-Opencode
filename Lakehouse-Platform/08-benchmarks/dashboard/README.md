# Benchmark Dashboard

> **Interactive visualization of Apache Arrow performance benchmarks**

---

## Overview

This directory contains an interactive HTML dashboard that visualizes benchmark results, showing the performance comparison between Apache Arrow and traditional row-based storage.

---

## Quick Start

### Option 1: Open Pre-built Dashboard

Simply open the HTML file in your browser:

```bash
# Windows
start benchmark-dashboard.html

# macOS
open benchmark-dashboard.html

# Linux
xdg-open benchmark-dashboard.html
```

### Option 2: Generate Dashboard from Results

```bash
# Generate from benchmark results
python generate-dashboard.py

# Generate with custom input/output
python generate-dashboard.py --input ../benchmark_results/benchmark_report.json --output my-dashboard.html
```

---

## Dashboard Features

### Summary Cards

| Metric | Description |
|--------|-------------|
| **Total Benchmarks** | Number of queries tested |
| **Average Speedup** | Overall performance improvement |
| **Data Analyzed** | Total data volume processed |
| **Time Saved** | Percentage of time saved with Arrow |

### Interactive Charts

1. **Execution Time Comparison**
   - Bar chart comparing query time with/without Arrow
   - Shows dramatic difference in milliseconds

2. **Speedup Factor**
   - Bar chart showing speedup multiplier for each query
   - Color-coded: Green (500x+), Blue (100-500x), Orange (10-100x)

3. **Data Scanned**
   - Doughnut chart showing data volume reduction
   - 500 GB vs 500 MB (1000x less data)

4. **Performance Distribution**
   - Pie chart showing distribution of speedup categories
   - Excellent, Good, Moderate performance levels

### Detailed Results Table

| Column | Description |
|--------|-------------|
| Query Name | Name of the benchmark query |
| Type | Query type (Aggregation, Window Functions, etc.) |
| Without Arrow | Execution time in milliseconds |
| With Arrow | Execution time with Arrow reflections |
| Speedup | Performance improvement multiplier |
| Rows Returned | Number of result rows |
| Status | Performance rating (Excellent/Good/Moderate) |

---

## Customization

### Adding Your Own Data

Edit the `benchmarkData` object in the HTML file:

```javascript
const benchmarkData = {
    queries: [
        {
            name: 'Your Query Name',
            type: 'Aggregation',
            withoutArrow: 45000,  // ms
            withArrow: 450,       // ms
            rowsReturned: 1000,
            dataScannedMB: 500
        },
        // Add more queries...
    ]
};
```

### Styling

The dashboard uses a dark theme with the following color scheme:

| Color | Usage |
|-------|-------|
| `#00d4ff` | Primary accent (cyan) |
| `#00ff88` | Success/Excellent (green) |
| `#ffaa00` | Warning/Moderate (orange) |
| `#ff4444` | Error/Poor (red) |
| `#1a1a2e` | Background dark |
| `#16213e` | Background light |

---

## Dashboard Sections

### Header
- Title and description
- Last updated timestamp

### Stats Grid
- 4 summary cards with key metrics

### Charts Section
- 2x2 grid of interactive charts
- Responsive design for all screen sizes

### Results Table
- Detailed benchmark results
- Sortable columns
- Color-coded status badges

### Footer
- Project information
- Generation timestamp

---

## Browser Compatibility

The dashboard works best in modern browsers:

| Browser | Status |
|---------|--------|
| Chrome 90+ | ✅ Full support |
| Firefox 88+ | ✅ Full support |
| Safari 14+ | ✅ Full support |
| Edge 90+ | ✅ Full support |
| IE 11 | ❌ Not supported |

---

## Troubleshooting

### Charts Not Loading

Ensure you have internet access for Chart.js CDN:

```html
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
```

### Data Not Showing

Check browser console for errors:
1. Open Developer Tools (F12)
2. Go to Console tab
3. Look for JavaScript errors

### Performance Issues

For large datasets:
1. Reduce number of queries in `benchmarkData`
2. Use pagination for tables
3. Consider lazy loading charts

---

## Integration with Benchmark Runner

The dashboard can be automatically generated after running benchmarks:

```bash
# Run benchmark and generate dashboard
cd ..
./run-benchmark.sh

# Then generate dashboard
cd dashboard
python generate-dashboard.py
```

---

## File Structure

```
dashboard/
├── README.md                    # This file
├── benchmark-dashboard.html     # Interactive dashboard
└── generate-dashboard.py        # Dashboard generator script
```

---

## Further Reading

- [Benchmark Tests](../README.md)
- [Arrow Reflections Tutorial](../../07-tutorials/arrow-reflections-tutorial.md)
- [Performance Optimization](../../README.md#9-performance-optimization)

---

*Created for: Banking Data Platform - Lakehouse Architecture*
*Last Updated: 2025-01-15*