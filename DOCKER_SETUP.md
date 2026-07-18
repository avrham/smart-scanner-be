# Smart Scanner Backend - Docker Setup

## 🚀 Quick Start

Your Smart Scanner backend is now fully containerized and running!

### Current Status ✅
- **API Server**: Running on `http://localhost:8000` (healthy)
- **Scheduler**: Monitoring for scan times (10:00, 14:00, 18:00 UTC)
- **Health Check**: Automated health monitoring with retry logic
- **Demo Mode**: Python 3.13 compatible without database dependencies

## 📋 Docker Commands

### Start the system
```bash
docker-compose up -d
```

### Check status
```bash
docker-compose ps
```

### View logs
```bash
# All services
docker-compose logs -f

# API only
docker-compose logs -f api

# Scheduler only
docker-compose logs -f scheduler
```

### Stop the system
```bash
docker-compose down
```

### Rebuild after code changes
```bash
docker-compose down
docker-compose build
docker-compose up -d
```

## 🔧 Configuration

### Environment Variables (.env)
- `WORKER_TOKEN`: Authentication token for admin endpoints
- `FMP_API_KEY`: Financial Modeling Prep API key
- `SUPABASE_URL` & `SUPABASE_SERVICE_KEY`: Database credentials (for future use)
- `SCAN_BATCH_SIZE`: Number of symbols to scan per batch (150)
- `SCAN_TIMES`: UTC hours for automated scanning (10, 14, 18)

### Scheduler Behavior
- Monitors current UTC time every 5 minutes outside scan hours
- Triggers scans precisely at minute 00 of scheduled hours
- Prevents duplicate triggers with 1-hour sleep after scan
- Automatically retries on API connectivity issues

## 🌐 API Endpoints

### Public Endpoints
- `GET /health` - System health check
- `GET /api/patterns` - Available patterns
- `GET /api/signals` - Recent ENTER signals
- `GET /api/pattern-runs` - Scan telemetry

### Admin Endpoints (requires X-Worker-Token header)
- `POST /api/admin/scan/start` - Manual scan trigger

### Test Commands
```bash
# Health check
curl http://localhost:8000/health

# Get patterns
curl http://localhost:8000/api/patterns

# Manual scan (requires token)
curl -X POST -H "X-Worker-Token: smart-scanner-worker-2024-demo-token" \
     http://localhost:8000/api/admin/scan/start
```

## 📊 Current Features

### ✅ Working
- FastAPI server with Python 3.13 support
- Automated scheduling with curl-based triggers
- Health monitoring with restart policies
- Demo endpoints with sample data
- CORS configuration for frontend integration
- Structured logging and error handling

### 🚧 Future (when asyncpg supports Python 3.13)
- Database connectivity for real signal storage
- FMP API integration for live data
- SMA-150 bounce pattern detection
- Supabase realtime integration
- Signal persistence and telemetry

## 🔧 Architecture

```
┌─────────────────────┐    ┌─────────────────────┐
│   API Container     │    │ Scheduler Container │
│                     │    │                     │
│ • FastAPI Server    │◄───┤ • Curl-based        │
│ • Health Checks     │    │ • Time monitoring   │
│ • Demo Endpoints    │    │ • Auto triggers     │
│ • Port 8000         │    │ • UTC scheduling    │
└─────────────────────┘    └─────────────────────┘
```

## 📝 Next Steps

1. **Frontend Integration**: Update frontend API base URL to `http://localhost:8000`
2. **Database Setup**: When asyncpg becomes Python 3.13 compatible, uncomment database service
3. **Production Deploy**: Use this setup as base for production deployment
4. **Monitoring**: Add external monitoring for production use

## 🎯 Demo Data

The system currently returns demo data for all endpoints:
- Sample SMA-150 bounce signal for AAPL
- Mock pattern configuration
- Simulated scan run statistics

This allows you to develop and test the frontend while waiting for full database integration.

---

**Status**: ✅ Fully operational in demo mode
**Next**: Frontend integration and database compatibility updates
