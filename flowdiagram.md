# Insurance Loan Data Pipeline

```mermaid
flowchart LR

    subgraph External Source
        S3[Amazon S3<br>JSON Files]
    end

    subgraph Snowflake Ingestion Layer
        SNOWPIPE[Snowpipe Auto-Ingest]
        RAW[TBL_ME_RAW]
        STREAM_RAW[STREAM_ME_RAW]
    end

    subgraph Transformation Layer
        TASK_RAW[TASK_ME_RAW_TO_STG]
        SP_FLATTEN[SP_FLATTEN_TO_STG]
        STG[TBL_ME_STG]
        STREAM_STG[STREAM_ME_STG]
    end

    subgraph Curated & Analysis Layer
        TASK_CURATED[TASK_ME_STG_TO_CURATED]
        SP_MISMATCH[SP_MISMATCH_ANALYSIS]
        CURATED[INSURANCE_LOAN_READER]
    end

    subgraph Logging & Audit
        LOG[TBL_DL_LOG]
    end

    %% Flow

    S3 --> SNOWPIPE
    SNOWPIPE --> RAW
    RAW --> STREAM_RAW

    STREAM_RAW --> TASK_RAW
    TASK_RAW --> SP_FLATTEN
    SP_FLATTEN --> STG

    STG --> STREAM_STG
    STREAM_STG --> TASK_CURATED
    TASK_CURATED --> SP_MISMATCH

    SP_MISMATCH --> CURATED
    SP_MISMATCH -->|Mismatch Analysis + Timestamp Update| CURATED

    TASK_RAW --> LOG
    TASK_CURATED --> LOG
