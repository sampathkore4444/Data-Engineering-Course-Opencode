#!/bin/bash
# ============================================================================
# Apache Arrow Performance Benchmark Runner
# ============================================================================
# This script runs the Arrow performance benchmark for banking data
#
# Usage:
#   ./run-benchmark.sh                    # Run full benchmark
#   ./run-benchmark.sh --quick            # Run quick benchmark (1M rows)
#   ./run-benchmark.sh --full             # Run full benchmark (10M rows)
#   ./run-benchmark.sh --report-only      # Generate report from existing results
#
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/benchmark_results"
VENV_DIR="${SCRIPT_DIR}/.venv"

# Parse arguments
QUICK_MODE=false
FULL_MODE=false
REPORT_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK_MODE=true
            shift
            ;;
        --full)
            FULL_MODE=true
            shift
            ;;
        --report-only)
            REPORT_ONLY=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}  APACHE ARROW PERFORMANCE BENCHMARK FOR BANKING                         ${NC}"
echo -e "${BLUE}========================================================================${NC}"

# ============================================================================
# STEP 1: Check Prerequisites
# ============================================================================

echo -e "\n${YELLOW}Step 1: Checking prerequisites...${NC}"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python 3 is not installed. Please install Python 3.8+${NC}"
    exit 1
fi

# Check Docker (for databases)
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker${NC}"
    exit 1
fi

# Check if Docker Compose is available
if ! docker compose version &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose v2${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites check passed${NC}"

# ============================================================================
# STEP 2: Setup Python Environment
# ============================================================================

echo -e "\n${YELLOW}Step 2: Setting up Python environment...${NC}"

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Install required packages
echo "Installing required packages..."
pip install -q psycopg2-binary mysql-connector-python pandas sqlalchemy tabulate

echo -e "${GREEN}✅ Python environment ready${NC}"

# ============================================================================
# STEP 3: Check Database Connectivity
# ============================================================================

echo -e "\n${YELLOW}Step 3: Checking database connectivity...${NC}"

# Check if PostgreSQL is running
if docker compose -f "${SCRIPT_DIR}/../01-docker-setup/docker-compose.yml" ps postgres-core-banking 2>/dev/null | grep -q "Up"; then
    echo -e "${GREEN}✅ PostgreSQL is running${NC}"
else
    echo -e "${YELLOW}⚠️  PostgreSQL is not running. Starting databases...${NC}"
    cd "${SCRIPT_DIR}/../01-docker-setup"
    docker compose up -d postgres-core-banking
    sleep 10
fi

# Check if MySQL is running
if docker compose -f "${SCRIPT_DIR}/../01-docker-setup/docker-compose.yml" ps mysql-credit-cards 2>/dev/null | grep -q "Up"; then
    echo -e "${GREEN}✅ MySQL is running${NC}"
else
    echo -e "${YELLOW}⚠️  MySQL is not running. Starting databases...${NC}"
    cd "${SCRIPT_DIR}/../01-docker-setup"
    docker compose up -d mysql-credit-cards
    sleep 10
fi

cd "$SCRIPT_DIR"

# ============================================================================
# STEP 4: Run Benchmark
# ============================================================================

echo -e "\n${YELLOW}Step 4: Running benchmark...${NC}"

# Create results directory
mkdir -p "$RESULTS_DIR"

# Determine test data size
if [ "$QUICK_MODE" = true ]; then
    TEST_ROWS=100000  # 100K rows
    ITERATIONS=2
    echo -e "${BLUE}Running QUICK benchmark (100K rows, 2 iterations)${NC}"
elif [ "$FULL_MODE" = true ]; then
    TEST_ROWS=10000000  # 10M rows
    ITERATIONS=5
    echo -e "${BLUE}Running FULL benchmark (10M rows, 5 iterations)${NC}"
else
    TEST_ROWS=1000000  # 1M rows (default)
    ITERATIONS=3
    echo -e "${BLUE}Running STANDARD benchmark (1M rows, 3 iterations)${NC}"
fi

# Run benchmark script
echo -e "\n${YELLOW}Executing benchmark...${NC}"

python3 arrow-performance-benchmark.py \
    --test-rows "$TEST_ROWS" \
    --iterations "$ITERATIONS" \
    --output-dir "$RESULTS_DIR"

# ============================================================================
# STEP 5: Display Results
# ============================================================================

echo -e "\n${YELLOW}Step 5: Displaying results...${NC}"

if [ -f "$RESULTS_DIR/benchmark_report.md" ]; then
    echo -e "\n${GREEN}✅ Benchmark report generated!${NC}"
    echo -e "\n${BLUE}Report location:${NC}"
    echo -e "  - Markdown: $RESULTS_DIR/benchmark_report.md"
    echo -e "  - JSON:     $RESULTS_DIR/benchmark_report.json"
    echo -e "  - CSV:      $RESULTS_DIR/benchmark_results.csv"
    
    echo -e "\n${BLUE}Quick Summary:${NC}"
    
    # Extract and display key metrics from markdown report
    if command -v grep &> /dev/null; then
        grep -A 10 "Executive Summary" "$RESULTS_DIR/benchmark_report.md" 2>/dev/null || true
    fi
else
    echo -e "${RED}❌ Benchmark report not found${NC}"
    exit 1
fi

# ============================================================================
# STEP 6: Open Report (Optional)
# ============================================================================

echo -e "\n${YELLOW}Step 6: Opening report...${NC}"

# Try to open report in default browser (if available)
if command -v xdg-open &> /dev/null; then
    xdg-open "$RESULTS_DIR/benchmark_report.md" 2>/dev/null || true
elif command -v open &> /dev/null; then
    open "$RESULTS_DIR/benchmark_report.md" 2>/dev/null || true
else
    echo -e "${YELLOW}ℹ️  Open the report manually:${NC}"
    echo -e "  $RESULTS_DIR/benchmark_report.md"
fi

# ============================================================================
# COMPLETE
# ============================================================================

echo -e "\n${GREEN}========================================================================${NC}"
echo -e "${GREEN}  BENCHMARK COMPLETE!                                                    ${NC}"
echo -e "${GREEN}========================================================================${NC}"

echo -e "\n${BLUE}Next Steps:${NC}"
echo -e "1. Review the benchmark report"
echo -e "2. Identify high-impact queries for reflection"
echo -e "3. Create Arrow reflections using the tutorial:"
echo -e "   ${SCRIPT_DIR}/../07-tutorials/arrow-reflections-tutorial.md"
echo -e "4. Re-run benchmark to verify improvements"

echo -e "\n${YELLOW}To run again:${NC}"
echo -e "  Quick:   ./run-benchmark.sh --quick"
echo -e "  Standard: ./run-benchmark.sh"
echo -e "  Full:    ./run-benchmark.sh --full"

echo ""