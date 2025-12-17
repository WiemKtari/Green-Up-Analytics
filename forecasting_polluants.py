import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import os
import warnings

warnings.filterwarnings("ignore")

# ============================
# 1️⃣ Connect to DW
# ============================
engine = create_engine(
    "postgresql://myuser:strong_password@localhost:5432/dwh_pollution"
)

df_air = pd.read_sql("""
SELECT 
    f.location_id,
    f.pm10, f.pm25, f.no2,
    t.year
FROM dw.fact_air_quality f
JOIN dw.dim_time t ON f.time_id = t.time_id
ORDER BY location_id, year
""", engine)

# ============================
# 2️⃣ Evaluation function
# ============================
def evaluate_sarima(series, years):
    """
    Train on all but last year, evaluate on last year
    """
    if len(series) < 5:
        return None  # SARIMA needs enough points

    train_y = series[:-1]
    test_y = series[-1]

    try:
        model = SARIMAX(
            train_y,
            order=(1,1,1),        # simple but robust
            seasonal_order=(0,0,0,0),
            enforce_stationarity=False,
            enforce_invertibility=False
        )
        res = model.fit(disp=False)
        pred = res.forecast(steps=1)[0]

        return {
            "mae": abs(test_y - pred),
            "rmse": np.sqrt((test_y - pred) ** 2),
            "r2": None  # R² not meaningful for single-step per series
        }
    except:
        return None

# ============================
# 3️⃣ Recursive forecast
# ============================
def sarima_forecast(series, start_year, end_year):
    """
    Recursive SARIMA forecast
    """
    forecasts = {}
    history = list(series)

    try:
        model = SARIMAX(
            history,
            order=(1,1,1),
            seasonal_order=(0,0,0,0),
            enforce_stationarity=False,
            enforce_invertibility=False
        )
        res = model.fit(disp=False)

        steps = end_year - start_year + 1
        preds = res.forecast(steps=steps)

        return preds.tolist()
    except:
        return None

# ============================
# 4️⃣ Run SARIMA per pollutant
# ============================
pollutants = ['pm10', 'pm25', 'no2']
start_forecast_year = df_air['year'].max() + 1
end_forecast_year = 2026

os.makedirs("forecast_results", exist_ok=True)

for pol in pollutants:
    print(f"\n=== SARIMA for {pol.upper()} ===")

    all_forecasts = []
    eval_mae, eval_rmse = [], []

    for location_id, group in df_air[['location_id','year',pol]].dropna().groupby('location_id'):
        group = group.sort_values('year')

        years = group['year'].values
        values = group[pol].values

        # -------- Evaluation --------
        eval_res = evaluate_sarima(values, years)
        if eval_res:
            eval_mae.append(eval_res['mae'])
            eval_rmse.append(eval_res['rmse'])

        # -------- Forecast --------
        preds = sarima_forecast(values, start_forecast_year, end_forecast_year)
        if preds is None:
            continue

        forecast_years = list(range(start_forecast_year, end_forecast_year + 1))

    n_preds = min(len(preds), len(forecast_years))

    for i in range(n_preds):
        all_forecasts.append({
            "location_id": location_id,
            "year": forecast_years[i],
            f"{pol}_pred": preds[i]
        })


    # -------- Report evaluation --------
    if eval_mae:
        print(
            f"Evaluation (mean over locations): "
            f"MAE={np.mean(eval_mae):.2f}, "
            f"RMSE={np.mean(eval_rmse):.2f}"
        )
    else:
        print("Not enough data for evaluation")

    # -------- Save forecast --------
    forecast_df = pd.DataFrame(all_forecasts)
    forecast_df.to_csv(
        f"forecast_results/{pol}_sarima_forecast_{start_forecast_year}_{end_forecast_year}.csv",
        index=False
    )

    print(f"Forecast saved for {pol}")

print("\nAll SARIMA forecasts completed.")
