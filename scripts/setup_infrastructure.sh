#!/bin/bash

set -e

echo "🚀 Setting up ClickHouse EDW Infrastructure..."
echo "=============================================="

# Colors for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Function to check service readiness
check_service() {
    local service_name=$1
    local url=$2
    local max_attempts=30
    local attempt=1

    print_status "Waiting for $service_name to be ready..."

    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            print_status "$service_name is ready!"
            return 0
        fi
        printf "   Attempt $attempt/$max_attempts failed, retrying in 10s...\r"
        sleep 10
        ((attempt++))
    done

    print_error "$service_name failed to start after $max_attempts attempts"
    return 1
}

# Function to check JSON response
check_json_service() {
    local service_name=$1
    local url=$2
    local max_attempts=20
    local attempt=1

    print_status "Waiting for $service_name to be ready..."

    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" | jq . > /dev/null 2>&1; then
            print_status "$service_name is ready!"
            return 0
        fi
        printf "   Attempt $attempt/$max_attempts failed, retrying in 15s...\r"
        sleep 15
        ((attempt++))
    done

    print_error "$service_name failed to start after $max_attempts attempts"
    return 1
}

# Step 1: Pre-flight checks
print_status "Running pre-flight checks..."

if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "docker-compose not found. Please install docker-compose."
    exit 1
fi

# Step 2: Create .env if not exists
if [ ! -f .env ]; then
    print_status "Creating .env file..."
    cp .env.example .env 2>/dev/null || print_warning ".env.example not found, using defaults"
fi

# Step 3: Create directories
print_status "Creating necessary directories..."
mkdir -p {airflow/logs,airflow/plugins,configs/clickhouse,dbt/target,scripts}

# Step 4: Generate new Fernet key
print_status "Generating Airflow Fernet key..."
if command -v python3 &> /dev/null; then
    python3 -c "
from cryptography.fernet import Fernet
key = Fernet.generate_key().decode()
print(f'Generated Fernet key: {key}')
with open('.env', 'a') as f:
    f.write(f'\nAIRFLOW_FERNET_KEY={key}\n')
"
else
    print_warning "Python3 not found, using default Fernet key"
fi

# Step 5: Create Excel filter file
print_status "Creating Excel filter file..."
python3 scripts/create_excel_filters.py

# Step 6: Clean up existing containers
print_status "Cleaning up existing containers..."
docker-compose down -v --remove-orphans 2>/dev/null || true

# Step 7: Build custom images
print_status "Building custom Docker images..."
docker-compose build

# Step 8: Start core infrastructure
print_status "Starting core infrastructure..."
docker-compose up -d zookeeper kafka postgres-airflow redis
sleep 20

# Step 9: Initialize Kafka topics (if kafka-init service exists)
print_status "Initializing Kafka infrastructure..."
docker-compose up -d kafka-init 2>/dev/null || print_status "No kafka-init service found, using auto-creation"
sleep 10

# Step 10: Start Schema Registry
print_status "Starting Schema Registry..."
docker-compose up -d schema-registry
sleep 15

# Check Schema Registry
check_json_service "Schema Registry" "http://localhost:8081/"

# Step 11: Start ClickHouse instances
print_status "Starting ClickHouse instances..."
docker-compose up -d clickhouse-source clickhouse-edw
sleep 30

# Step 12: Check ClickHouse readiness
check_service "ClickHouse Source" "http://localhost:8123/ping"
check_service "ClickHouse EDW" "http://localhost:8124/ping"

# Step 13: Start Debezium Connect
print_status "Starting Debezium Kafka Connect..."
docker-compose up -d debezium
sleep 30

# Check Debezium Connect
check_json_service "Debezium Connect" "http://localhost:8083/"

print_status "Registering CDC connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @./debezium/connectors/source-connector.json


# Step 14: Start Kafka UI (if exists)
print_status "Starting Kafka UI..."
docker-compose up -d kafka-ui 2>/dev/null || print_status "No kafka-ui service found"
sleep 10

# Check Kafka UI if exists
curl -s "http://localhost:8090/" > /dev/null 2>&1 && print_status "Kafka UI is ready!" || print_status "Kafka UI not available"

# Step 15: Initialize Airflow database
print_status "Initializing Airflow database..."
docker-compose run --rm airflow-webserver airflow db init

# Step 16: Create Airflow admin user
print_status "Creating Airflow admin user..."
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Step 17: Start Airflow services
print_status "Starting Airflow services..."
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker

# Step 18: Check Airflow readiness
check_service "Airflow" "http://localhost:8080/health"

# Step 19: Initialize EDW database structure
print_status "Initializing EDW database structure..."
sleep 10

# Step 20: Test connections and show status
print_status "Testing connections..."

SOURCE_COUNT=$(docker exec clickhouse-source clickhouse-client --query="SELECT count(*) FROM staging.products" 2>/dev/null || echo "0")
EDW_COUNT=$(docker exec clickhouse-edw clickhouse-client --query="SELECT count(*) FROM edw.stg_products" 2>/dev/null || echo "0")

print_status "ClickHouse Source - Sample records: $SOURCE_COUNT"
print_status "ClickHouse EDW - Staging records: $EDW_COUNT"

# Verify Kafka Connect plugins
JDBC_PLUGIN=$(curl -s "http://localhost:8083/connector-plugins" | jq -r '.[] | select(.class | contains("JdbcSourceConnector")) | .class' 2>/dev/null || echo "")

if [ -n "$JDBC_PLUGIN" ]; then
    print_status "✅ Debezium JDBC Connector found: $JDBC_PLUGIN"
else
    print_warning "⚠️  Debezium JDBC Connector not found, CDC may not work"
fi

# Final status
echo ""
print_status "🎉 Infrastructure setup completed successfully!"
echo ""
echo -e "${BLUE}📋 Access Information:${NC}"
echo "   🌐 Airflow UI: http://localhost:8080"
echo "   👤 Username: admin"
echo "   🔑 Password: admin"
echo ""
echo "   📊 ClickHouse Source: http://localhost:8123"
echo "   📊 ClickHouse EDW: http://localhost:8124"
echo "   🔗 Kafka Connect: http://localhost:8083"
if curl -s "http://localhost:8090/" > /dev/null 2>&1; then
echo "   📈 Kafka UI: http://localhost:8090"
fi
echo "   🔧 Schema Registry: http://localhost:8081"
echo ""
echo -e "${BLUE}📋 Next Steps:${NC}"
echo "   1. ✅ Infrastructure is ready"
echo "   2. 🌐 Access Airflow UI and verify DAGs are loaded"
echo "   3. ▶️  Trigger 'initial_load_pipeline' DAG manually"
echo "   4. 🔗 Deploy CDC connector if needed"
echo "   5. 📈 Monitor execution in Airflow logs"
echo "   6. 📝 Add new deals to configs/deal_filters.xlsx"
echo ""
echo -e "${BLUE}🔍 Quick Health Check:${NC}"
echo "   Run: ./scripts/monitor.sh"
