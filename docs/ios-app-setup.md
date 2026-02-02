# Health Auto Export Configuration

## REST API (Recommended)

Configure the iOS app to send data via REST API through Cloudflare Tunnel:

1. Open **Health Auto Export** app on your iPhone
2. Go to **Settings** -> **Automations**
3. Create a new automation:
   - **Trigger**: Daily at 23:00 (or your preferred time)
   - **Export Format**: JSON
   - **Destination**: REST API
4. Configure REST API settings:
   - **URL**: `https://health-api.yourdomain.com/ingest`
   - **Method**: POST
   - **Headers**: `Authorization: Bearer <your-HTTP_AUTH_TOKEN>`
5. Select metrics to export (see list below)

## Recommended Metrics

   - Heart Rate, Resting Heart Rate, HRV
   - Steps, Active Energy, Exercise Time
   - Sleep Analysis
   - Workouts
   - Weight, Body Fat
   - Blood Oxygen, Respiratory Rate
