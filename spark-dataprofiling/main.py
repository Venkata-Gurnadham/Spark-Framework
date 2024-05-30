import sys
from profiling.numeric_profiling import run_numeric_profiling
from profiling.categorical_profiling import run_categorical_profiling
from frontend.data_profiling.st_categorical_profiling import display_profile_report
import streamlit as st

# if __name__ == "__main__":
#     config_path = "config/config.yaml"
#     run_numeric_profiling(config_path)
#     run_categorical_profiling(config_path)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <config_path>")
        sys.exit(1)
    config_path = sys.argv[1]
    run_categorical_profiling(config_path)
    run_numeric_profiling(config_path)
    categorical_profile_report = run_categorical_profiling(config_path)
    display_profile_report(categorical_profile_report)
