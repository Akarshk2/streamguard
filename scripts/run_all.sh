#!/bin/bash
# =============================================================================
# StreamGuard — Full Stack Startup Script
# Run this after completing the Prerequisites in README.md
# =============================================================================

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { echo -e "${GREEN}[StreamGuard]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║        StreamGuard — Starting Stack          ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════╝${NC}"
echo ""

# ── Check .env ────────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
    warn ".env not found. Copying from .env.example — fill in your credentials!"
    cp .env.example .env
fi

source .env

# ── Step 1: Start Docker stack ────────────────────────────────────────────────
log "Step 1/6 — Starting Docker stack (Kafka, Zookeeper, Airflow, API)..."
docker-compose up -d
sleep 15
log "Docker stack started. Kafka UI: http://localhost:8090 | Airflow: http://localhost:8080"

# ── Step 2: Create Kafka topics ───────────────────────────────────────────────
log "Step 2/6 — Creating Kafka topics..."
sleep 10  # wait for Kafka to be ready
bash kafka/topic_setup.sh

# ── Step 3: Create Python venv ────────────────────────────────────────────────
log "Step 3/6 — Setting up Python environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install -q -r requirements.txt
log "Python environment ready"

# ── Step 4: Snowflake DDL ─────────────────────────────────────────────────────
log "Step 4/6 — NOTE: Run infra/snowflake_ddl.sql in your Snowflake worksheet if not done already."
warn "          Skipping auto-DDL (requires Snowflake credentials). Manual step required."

# ── Step 5: dbt setup ─────────────────────────────────────────────────────────
log "Step 5/6 — Setting up dbt..."
cd dbt
dbt deps --profiles-dir . 2>/dev/null || warn "dbt deps failed — check profiles.yml credentials"
cd ..

# ── Step 6: Start simulator ───────────────────────────────────────────────────
log "Step 6/6 — Starting data simulator..."
echo ""
echo -e "${GREEN}════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  All services started!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════${NC}"
echo ""
echo "  Kafka UI   → http://localhost:8090"
echo "  Airflow    → http://localhost:8080  (admin/admin)"
echo "  FastAPI    → http://localhost:8000/docs"
echo ""
echo "  Next steps:"
echo "  1. Start PySpark consumers:  spark-submit streaming/consumer_orders.py"
echo "  2. Train ML model:           python ml/train_model.py   (after ~2hrs of data)"
echo "  3. Start inference:          python ml/inference.py"
echo "  4. Run dbt:                  cd dbt && dbt run"
echo ""

python simulator/data_simulator.py --fault-rate 0.08
