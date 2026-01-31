# Data Model

## InfluxDB Schema

**Organization**: `health` | **Bucket**: `apple_health` | **Retention**: Infinite

| Measurement | Tags | Fields |
|-------------|------|--------|
| `heart` | source | bpm, bpm_min, bpm_max, bpm_avg, resting_bpm, hrv_ms, hrv_ms_min, hrv_ms_max, hrv_ms_avg |
| `activity` | source | steps, active_calories, basal_calories, distance_m, exercise_min, stand_min, stand_hours, floors_climbed |
| `sleep` | source | duration_min, deep_min, rem_min, core_min, awake_min, in_bed_min, quality_score |
| `workout` | source, workout_type | duration_min, calories, distance_m, avg_hr, max_hr |
| `body` | source | weight_kg, body_fat_pct, bmi, lean_mass_kg, waist_cm, height_cm |
| `vitals` | source | spo2_pct, spo2_pct_min, spo2_pct_max, respiratory_rate, bp_systolic, bp_diastolic, temp_c, vo2max |
| `mobility` | source | speed_mps, step_length_cm, asymmetry_pct, double_support_pct, stair_ascent_speed, stair_descent_speed, six_min_walk_m, steadiness_pct |
| `audio` | source | headphone_db, environmental_db |
| `other` | source, metric_type, unit | value, min, max, avg |

## Supported Workout Types

Running, Walking, Cycling, Swimming, Strength Training, HIIT, Yoga, Pilates, Elliptical, Rowing, Stair Climbing, Core Training, Flexibility, and more.
