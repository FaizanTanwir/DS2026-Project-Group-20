import numpy as np
from sklearn.ensemble import IsolationForest
import joblib

# Generate synthetic normal data
normal_data = np.random.normal(loc=50, scale=10, size=(1000, 2))

# Train anomaly detection model
model = IsolationForest(contamination=0.05, random_state=42)
model.fit(normal_data)

# Save model
joblib.dump(model, "Project/ml_models/anomaly_model.pkl")

print("Model trained and saved.")
