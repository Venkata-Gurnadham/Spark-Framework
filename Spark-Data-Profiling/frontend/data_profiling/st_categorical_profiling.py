import streamlit as st
# import json

def display_profile_report(profile_report):
    st.title("Categorical Data Profiling Report")
    for column_report in profile_report['categorical_columns']:
        st.subheader(f"Column: {column_report['column_name']}")
        st.write(f"Total Records: {column_report['total_records']}")
        st.write(f"Distinct Count: {column_report['distinct_count']}")
        st.write(f"Zero Count: {column_report['zero_count']}")
        st.write(f"Null Count: {column_report['null_count']}")
        st.write(f"Percent Missing: {column_report['percent_missing']:.2f}%")
        
        st.write("Value Counts:")
        st.json(column_report['value_counts'])

        st.write(f"Mode: {column_report['mode']}")
        st.write("Top Values:")
        st.json(column_report['top_values'])

# def save_profile_report(profile_report, output_path):
#     with open(output_path, 'a') as f:
#         f.write(json.dumps(profile_report))
#         f.write('\n')

#     st.success("Profiling report saved successfully!")
