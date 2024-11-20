def clean_file(input_file, output_file):
    """
    Cleans up the given text file by removing empty lines or lines containing only whitespace.

    Args:
        input_file (str): Path to the input text file.
        output_file (str): Path to the cleaned output text file.

    Example Usage:
        clean_file("input.txt", "output.txt")
    """
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                # Write the line only if it is not empty or contains only whitespace
                if line.strip():  # `line.strip()` removes leading/trailing whitespace
                    outfile.write(line)
        print(f"Cleaned file written to: {output_file}")
    except FileNotFoundError:
        print(f"Error: The file {input_file} does not exist.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Example call to the function
# clean_file("new_code/extended_event_log.xes", "new_code/extended_event_log_no_empty_lines.xes")




CALL {
    MATCH (n)
    RETURN 'TotalNodes' AS Metric, NULL AS Detail, COUNT(n) AS Value
    UNION ALL
    MATCH ()-[r]->()
    RETURN 'TotalRelationships' AS Metric, NULL AS Detail, COUNT(r) AS Value
    UNION ALL
    MATCH (n)
    RETURN 'NodeCountByLabel' AS Metric, labels(n)[0] AS Detail, COUNT(n) AS Value
    UNION ALL
    MATCH ()-[r]->()
    RETURN 'RelationshipCountByType' AS Metric, TYPE(r) AS Detail, COUNT(r) AS Value
    UNION ALL
    MATCH (n)
    OPTIONAL MATCH (n)--()
    WITH n, COUNT(*) AS degree
    RETURN 'MaxNodeDegree' AS Metric, NULL AS Detail, MAX(degree) AS Value
    UNION ALL
    MATCH (n)
    OPTIONAL MATCH (n)--()
    WITH n, COUNT(*) AS degree
    RETURN 'AvgNodeDegree' AS Metric, NULL AS Detail, AVG(degree) AS Value
}
RETURN Metric, Detail, Value
ORDER BY Metric, Value DESC;




import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from sklearn.metrics import mean_squared_error, r2_score

# Load the data
file_path = 'new_code/results/durations_extended_event_log_no_empty_lines.xes.csv'
data = pd.read_csv(file_path)
durations = data.iloc[:, 0]

# Prepare data for fitting
x = np.arange(len(durations))
y = durations.values

# Normalize x values
x_normalized = x / max(x)

# Define linear and exponential functions with start at 0
def linear_model(x, m):
    return m * x

def exponential_model(x, a, b):
    return a * (np.exp(b * x) - 1)

# Fit models with bounds for exponential fitting
linear_params, _ = curve_fit(linear_model, x_normalized, y)
exponential_params, _ = curve_fit(
    exponential_model, x_normalized, y, bounds=(0, [np.inf, 1])
)

# Predictions
y_linear_pred = linear_model(x_normalized, *linear_params)
y_exponential_pred = exponential_model(x_normalized, *exponential_params)

# Calculate metrics
linear_mse = mean_squared_error(y, y_linear_pred)
exponential_mse = mean_squared_error(y, y_exponential_pred)

linear_r2 = r2_score(y, y_linear_pred)
exponential_r2 = r2_score(y, y_exponential_pred)

# Log transformation
y_log = np.log(y + 1)  # Add 1 to avoid log(0)
log_fit_params, _ = curve_fit(linear_model, x_normalized, y_log)
y_log_pred = linear_model(x_normalized, *log_fit_params)

# Plotting
plt.figure(figsize=(15, 10))

# Original data and fits
plt.subplot(2, 2, 1)
plt.plot(x, y, label='Original Data', marker='.', linestyle='none')
plt.plot(x, y_linear_pred, label=f'Linear Fit (MSE={linear_mse:.4f}, R2={linear_r2:.4f})')
plt.plot(x, y_exponential_pred, label=f'Exponential Fit (MSE={exponential_mse:.4f}, R2={exponential_r2:.4f})')
plt.title('Original Data and Fitted Models')
plt.xlabel('Event Index')
plt.ylabel('Duration')
plt.legend()

# Log-transformed data
plt.subplot(2, 2, 2)
plt.plot(x, y_log, label='Log-Transformed Data', marker='.')
plt.plot(x, y_log_pred, label='Linear Fit on Log Data')
plt.title('Log-Transformed Data with Linear Fit')
plt.xlabel('Event Index')
plt.ylabel('Log(Duration)')
plt.legend()

# Residual plots
linear_residuals = y - y_linear_pred
exponential_residuals = y - y_exponential_pred

plt.subplot(2, 2, 3)
plt.scatter(x, linear_residuals, label='Linear Model Residuals', alpha=0.6)
plt.axhline(0, color='red', linestyle='--')
plt.title('Linear Model Residuals')
plt.xlabel('Event Index')
plt.ylabel('Residuals')

plt.subplot(2, 2, 4)
plt.scatter(x, exponential_residuals, label='Exponential Model Residuals', alpha=0.6)
plt.axhline(0, color='red', linestyle='--')
plt.title('Exponential Model Residuals')
plt.xlabel('Event Index')
plt.ylabel('Residuals')

plt.tight_layout()
plt.show()
