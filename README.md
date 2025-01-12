# Haensel_AMS_Assignment

```pyhon 
python src/pipeline.py --start_date 2024-01-01 --end_date 2024-01-31
```


## Overview

- This report outlines the design of the attribution pipeline, the assumptions made during its implementation, and potential improvements.
- However, no data was found in the provided `challenge.db` file. Consequently, data extraction and uploading data to the API were not achieved. 
- The remaining pipeline stages have been implemented as per the requirements.

---

## Pipeline Design

### **Structure**

The pipeline is divided into distinct stages:

1. **Database Setup**

   - Establishes SQLite connection and ensures required tables are created.

2. **Data Extraction**

   - Queries customer journey data from `session_sources` and `conversions`.
   - Supports optional `start_date` and `end_date` filters.

3. **Data Transformation**

   - Prepares data into API-compatible formats, grouped by `conv_id`.
   - Splits payloads into chunks to meet API limits.

4. **API Integration**

   - Sends transformed data to the IHC API and retrieves attribution values.
   - Includes basic error handling for API failures.

5. **Data Loading**

   - Stores API responses in the `attribution_customer_journey` table.

6. **Reporting**

   - Aggregates data to populate the `channel_reporting` table.
   - Computes metrics like `CPO` and `ROAS`.

7. **Export**

   - Exports the final `channel_reporting` table to a CSV file.

---

## Assumptions

**API Reliability**

   - Assumed the IHC API responds reliably and adheres to documented limits.

**Time-Range Input**

   - Assumed filtering applies primarily to `session_sources`.

**Attribution Logic**

   - Assumed `ihc` values sum to 1 (100%) per `conv_id`.


---

## Improvements

**Parallel Processing**

   - Use parallel API requests to speed up processing.

**Retry Logic**

   - Add retry mechanisms for failed API calls.

**Monitoring and Logging**

   - Integrate tools like Prometheus for tracking performance.

**Validation Checks**

   - Add schema validation for input data.

**Deployment Readiness**

   - Containerize the pipeline and use orchestration tools like Airflow.

**Scalability**

   - Enable distributed processing for larger workloads.

---

## Conclusion

The pipeline meets the assignment requirements with a modular, extensible design. However, due to the absence of data in the `challenge.db` file, data extraction and API integration could not be tested. Future enhancements can improve performance, scalability, and robustness, making it suitable for large-scale applications.

