# Smart Scanner Backend

Advanced stock pattern detection and signal generation system using FastAPI and Supabase.

## Overview

The Smart Scanner Backend is a Python-based system that:

- Scans stocks using FMP (Financial Modeling Prep) API
- Detects patterns (starting with SMA-150 bounce)
- Generates actionable trading signals
- Provides real-time updates via Supabase
- Includes automated scheduling and rate limiting

## Features

### Pattern Detection
- **SMA-150 Bounce Pattern**: Identifies stocks that respect their 150-day moving average
- Configurable parameters for each pattern
- Deterministic scoring with detailed explanations
- Historical bounce analysis

### Data Management
- Real-time data from FMP API
- Rate-limited async requests (≤250 calls/min)
- Automatic duplicate detection (daily seen cache)
- Comprehensive telemetry and logging

### Scheduling
- Automated scans 3x daily (10:00, 14:00, 18:00 UTC)
- Configurable batch sizes and timing
- Background maintenance tasks
- Manual scan triggers via admin API

## Quick Start

### 1. Installation

```bash
# Clone repository
cd smart-scanner-be

# Install dependencies (no virtual environment needed per user preference)
pip install -r requirements.txt
```

### 2. Environment Setup

```bash
# Copy environment template
cp .env.template .env

# Edit .env with your credentials
# Required: FMP_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY, WORKER_TOKEN
```

### 3. Database Setup

**Note**: Per user preference, manually create database tables (no auto-creation):

```bash
# Run the SQL migration manually in your Supabase dashboard
# File: app/db/migrations/001_initial_schema.sql
```

### 4. Run the Application

```bash
# Start the server (user prefers python main.py)
python main.py
```

The API will be available at `http://localhost:8000`

## Configuration

### Environment Variables

Key environment variables (see `.env.template`):

```bash
# FMP API
FMP_API_KEY=your-fmp-api-key-here
FMP_RATE_LIMIT_PER_MIN=250

# Database
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-key

# Worker
WORKER_TOKEN=your-secure-token
SCAN_BATCH_SIZE=150
SCAN_TIMES=["10:00", "14:00", "18:00"]

# Debug
DEBUG_SAVE_AVOID=false  # Set to true to also store AVOID signals (for R&D)
```

### Pattern Configuration

SMA-150 Bounce pattern parameters are stored in the database and can be customized:

```sql
-- Update pattern configuration
UPDATE pattern_configs 
SET value = '200'::jsonb 
WHERE pattern_code = 'sma150_bounce' AND key = 'sma_window';
```

## API Endpoints

### Public Endpoints (Read-only)
- `GET /api/patterns` - List available patterns
- `GET /api/signals` - Get ENTER signals (paginated)
- `GET /api/signals/{id}` - Get specific signal details
- `GET /api/pattern-runs` - Get scan run telemetry
- `GET /health` - System health check

### Admin Endpoints (Worker token required)
- `POST /api/admin/scan/start` - Trigger manual scan
- `POST /api/admin/tickers/refresh` - Refresh ticker cache
- `POST /api/admin/maintenance/reset-daily-seen` - Clean up cache
- `GET /api/admin/status` - System status and statistics

### Authentication

Admin endpoints require `X-Worker-Token` header:

```bash
curl -H "X-Worker-Token: your-worker-token" \
     -X POST http://localhost:8000/api/admin/scan/start
```

## Architecture

### Core Components

1. **FMP Client** (`app/workers/fmp_client.py`)
   - Rate-limited async HTTP client
   - Automatic retries and backoff
   - Batch processing capabilities

2. **Pattern Detection** (`app/workers/patterns/`)
   - Modular pattern algorithms
   - SMA-150 bounce implementation
   - Extensible for additional patterns

3. **Scan Runner** (`app/workers/scan_runner.py`)
   - Orchestrates batch scanning
   - Manages concurrency and error handling
   - Persistence and telemetry

4. **Scheduler** (`app/workers/scheduler.py`)
   - Automated scan scheduling
   - Maintenance task execution
   - APScheduler-based cron jobs

### Data Flow

1. **Ticker Selection**: Random sampling from filtered universe
2. **Data Fetch**: Historical prices from FMP API
3. **Pattern Analysis**: Technical indicator calculation and pattern detection
4. **Signal Generation**: Scoring and verdict determination
5. **Persistence**: Store ENTER signals (AVOID signals only in debug mode)
6. **Real-time Updates**: Supabase real-time notifications

### Database Schema

Key tables:
- `patterns` - Pattern definitions
- `pattern_configs` - Pattern parameters
- `signals` - Generated signals (ENTER only for SMA-150)
- `pattern_runs` - Scan telemetry
- `daily_seen` - Duplicate prevention cache
- `tickers` - Symbol universe

## Performance & Scaling

### Rate Limiting
- FMP API: 250 calls/minute max
- Configurable concurrent requests (default: 10)
- Exponential backoff on rate limit errors

### Memory Usage
- Streaming data processing
- Configurable batch sizes
- Automatic cleanup of old data

### Database Optimization
- Indexes on frequently queried columns
- Partial indexes for ENTER signals
- Regular maintenance tasks

## Monitoring & Logging

### Structured Logging
```bash
# JSON format (production)
LOG_FORMAT=json
LOG_LEVEL=INFO

# Text format (development)
LOG_FORMAT=text
LOG_LEVEL=DEBUG
```

### Key Metrics
- Scan batch statistics (scanned/enter/rejected)
- Pattern run telemetry
- API response times
- Database connection health

### Health Checks
```bash
# Basic health
curl http://localhost:8000/health

# Admin status (requires token)
curl -H "X-Worker-Token: token" http://localhost:8000/api/admin/status
```

## Development

### Adding New Patterns

1. Create pattern detector in `app/workers/patterns/`
2. Add pattern configuration to database
3. Update scan runner to include new pattern
4. Add tests for pattern logic

### Testing

```bash
# Run pattern detection on sample data
python -m app.workers.patterns.sma150

# Test API endpoints
curl http://localhost:8000/api/patterns
```

## Deployment

### Production Checklist

1. Set `ENVIRONMENT=production`
2. Configure production database
3. Set secure `WORKER_TOKEN`
4. Enable scheduler (`ENABLE_SCHEDULER=true`)
5. Set appropriate `LOG_LEVEL=INFO`
6. Configure CORS origins

### Recommended Infrastructure
- **API**: Render, Railway, or Fly.io
- **Database**: Supabase (PostgreSQL)
- **Monitoring**: Built-in health endpoints + external monitoring

## Troubleshooting

### Common Issues

1. **FMP Rate Limiting**
   - Reduce `FMP_MAX_CONCURRENT` 
   - Lower `SCAN_BATCH_SIZE`
   - Check API key quota

2. **Database Connection**
   - Verify Supabase credentials
   - Check connection pool settings
   - Monitor database performance

3. **Missing Signals**
   - Check pattern configuration
   - Verify market filters
   - Review scan logs for errors

### Debug Mode

Enable debug signal storage:
```bash
DEBUG_SAVE_AVOID=true
```

This stores both ENTER and AVOID signals for analysis.

## Support

For issues and questions:
1. Check application logs
2. Review health check endpoints
3. Verify environment configuration
4. Check database table status
