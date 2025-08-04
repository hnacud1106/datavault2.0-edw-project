#!/bin/bash

echo "🔍 ClickHouse EDW System Status"
echo "==============================="

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_ok() { echo -e "${GREEN}✅${NC} $1"; }
print_error() { echo -e "${RED}❌${NC} $1"; }
print_warning() { echo -e "${YELLOW}⚠️${NC} $1"; }

echo "📦 Container Status:"
docker-compose ps

echo ""
echo "🏥 Service Health Checks:"

# ClickHouse Source
if curl -s http://localhost:8123/ping > /dev/null 2>&1; then
    COUNT=$(docker exec clickhouse-source clickhouse-client --query="SELECT count(*) FROM staging.products" 2>/dev/null || echo "N/A")
    print_ok "ClickHouse Source: $COUNT sample records"
else
    print_error "ClickHouse Source: Unreachable"
fi

# ClickHouse EDW
if curl -s http://localhost:8124/ping > /dev/null 2>&1; then
    STAGING_COUNT=$(docker exec clickhouse-edw clickhouse-client --query="SELECT count(*) FROM edw.stg_products" 2>/dev/null || echo "N/A")
    HUB_PRODUCT_COUNT=$(docker exec clickhouse-edw clickhouse-client --query="SELECT count(*) FROM edw.hub_product" 2>/dev/null || echo "N/A")
    HUB_DEAL_COUNT=$(docker exec clickhouse-edw clickhouse-client --query="SELECT count(*) FROM edw.hub_deal" 2>/dev/null || echo "N/A")

    print_ok "ClickHouse EDW: $STAGING_COUNT staging records"
    print_ok "Hub Product: $HUB_PRODUCT_COUNT records"
    print_ok "Hub Deal: $HUB_DEAL_COUNT records"
else
    print_error "ClickHouse EDW: Unreachable"
fi

# Airflow
if curl -s http://localhost:8080/health | grep -q "healthy"; then
    print_ok "Airflow: Healthy"
else
    print_warning "Airflow: Check required - http://localhost:8080"
fi

# Kafka
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    print_ok "Kafka: Available"
else
    print_warning "Kafka: Check required"
fi

echo ""
echo "💾 Storage Usage:"
docker system df

echo ""
echo "🎯 Resource Usage:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" | head -10

echo ""
echo "📊 Recent Logs (last 10 lines):"
echo "Airflow Scheduler:"
docker-compose logs --tail=3 airflow-scheduler | tail -3

echo ""
echo "ClickHouse EDW:"
docker-compose logs --tail=3 clickhouse-edw | tail -3

echo ""
echo "🔧 Quick Commands:"
echo "   View all logs: docker-compose logs -f"
echo "   Restart service: docker-compose restart <service>"
echo "   Access ClickHouse: docker exec -it clickhouse-edw clickhouse-client"
echo "   Access Airflow: http://localhost:8080 (admin/admin)"
