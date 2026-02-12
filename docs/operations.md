# Operations

## Troubleshooting

### No data appearing in Grafana

1. Check if health-ingest is receiving messages:
   ```bash
   docker logs -f health-ingest
   ```

2. Test the REST API endpoint:
   ```bash
   curl -s http://localhost:8084/health
   curl -X POST http://localhost:8084/ingest \
     -H "Authorization: Bearer <token>" \
     -H "Content-Type: application/json" \
     -d '{"data":[{"name":"heart_rate","date":"2024-01-15T10:30:00Z","qty":72}]}'
   ```

3. Check InfluxDB has data:
   ```bash
   docker exec -it health-influxdb influx query \
     'from(bucket:"apple_health") |> range(start:-1h) |> limit(n:10)'
   ```

### Health Auto Export not sending data

1. **REST API**: Verify the URL and Bearer token are correct in the iOS app settings
2. Verify automation is enabled and scheduled correctly
3. Try manual export to test connectivity

### InfluxDB connection errors

1. Verify token is correct in `.env`
2. Check InfluxDB container is healthy:
   ```bash
   docker exec health-influxdb influx ping
   ```

### Weekly report errors

1. Verify the provider API key (e.g., `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or `GROK_API_KEY`) is set correctly
2. Check you have sufficient API credits
3. View detailed logs:
   ```bash
   APP_LOG_LEVEL=DEBUG uv run health-report
   ```

## Security Notes

- Keep `HTTP_AUTH_TOKEN` secret. Treat it like a password and rotate it if it leaks.
- Keep `BOT_WEBHOOK_TOKEN` secret if bot endpoints are enabled.
- `HTTP_ALLOW_UNAUTHENTICATED=true` and `BOT_ALLOW_UNAUTHENTICATED_WEBHOOK=true` are
  development-only overrides. Do not use them in production.
- Only expose ports (8084, 8087, 3001) to trusted networks. If you need remote access, prefer a reverse proxy or Cloudflare Tunnel with authentication.
- Grafana and InfluxDB passwords are stored in `.env`; keep that file out of version control.

## Backups and Restore

Because all state lives in mounted volumes, backups are straightforward:

1. Stop services: `docker compose down`
2. Archive `/mnt/nvme/health` (or your custom data directory).
3. Restore by unpacking the archive to the same location and restarting the stack.

For a more granular approach, InfluxDB also supports native backup/restore commands (`influx backup` and `influx restore`).

## Cloudflare Tunnel Setup (Optional)

To expose services publicly via Cloudflare:

1. Add routes in [Cloudflare Zero Trust Dashboard](https://one.dash.cloudflare.com/):
   - **Hostname**: `health.yourdomain.com` -> `http://localhost:3001` (Grafana)
   - **Hostname**: `health-api.yourdomain.com` -> `http://localhost:8084` (REST API)

2. Configure Grafana root URL in `.env`:
   ```
   GRAFANA_ROOT_URL=https://health.yourdomain.com
   ```
