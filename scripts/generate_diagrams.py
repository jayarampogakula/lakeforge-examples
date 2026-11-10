import os
from PIL import Image, ImageDraw, ImageFont

# Set up color palette
BG_COLOR = (11, 19, 43)          # Dark navy blue
CARD_BG = (20, 32, 64)           # Deep card blue
CARD_BORDER = (45, 75, 130)       # Accent border
TEXT_MAIN = (255, 255, 255)      # White
TEXT_MUTED = (160, 175, 205)     # Soft blue-gray
TEXT_ACCENT = (56, 189, 248)     # Cyan
BADGE_BG = (30, 58, 138)         # Navy badge
BADGE_BORDER = (59, 130, 246)    # Blue badge border
BADGE_TEXT = (147, 197, 253)     # Light blue
STAGE_NUM_BG = (14, 165, 233)    # Bright cyan circle
ARROW_COLOR = (96, 165, 250)     # Connecting arrow

def get_fonts():
    try:
        title_font = ImageFont.truetype("arialbd.ttf", 28)
        subtitle_font = ImageFont.truetype("arial.ttf", 16)
        pill_font = ImageFont.truetype("arialbd.ttf", 13)
        box_header_font = ImageFont.truetype("arialbd.ttf", 16)
        box_body_font = ImageFont.truetype("arial.ttf", 13)
        arrow_font = ImageFont.truetype("arialbd.ttf", 11)
        footer_font = ImageFont.truetype("arial.ttf", 13)
    except Exception:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
        pill_font = ImageFont.load_default()
        box_header_font = ImageFont.load_default()
        box_body_font = ImageFont.load_default()
        arrow_font = ImageFont.load_default()
        footer_font = ImageFont.load_default()
    return title_font, subtitle_font, pill_font, box_header_font, box_body_font, arrow_font, footer_font

def draw_diagram(output_path, badge_text, title, subtitle, stages, arrow_labels, footer_text):
    W, H = 1200, 675
    img = Image.new("RGB", (W, H), color=BG_COLOR)
    draw = ImageDraw.Draw(img)
    fonts = get_fonts()
    title_font, subtitle_font, pill_font, box_header_font, box_body_font, arrow_font, footer_font = fonts

    # 1. Subtle decorative grid / border
    draw.rectangle([15, 15, W - 15, H - 15], outline=(25, 42, 86), width=2)
    draw.line([30, 135, W - 30, 135], fill=(30, 55, 105), width=1)
    draw.line([30, H - 55, W - 30, H - 55], fill=(30, 55, 105), width=1)

    # 2. Top Header Badge
    draw.rounded_rectangle([40, 32, 330, 62], radius=15, fill=BADGE_BG, outline=BADGE_BORDER, width=2)
    draw.text((55, 38), badge_text, fill=BADGE_TEXT, font=pill_font)

    # 3. Title & Subtitle
    draw.text((40, 72), title, fill=TEXT_MAIN, font=title_font)
    draw.text((40, 108), subtitle, fill=TEXT_MUTED, font=subtitle_font)

    # Watermark branding top-right
    draw.text((W - 250, 42), "LAKEFORGE FRAMEWORK", fill=(100, 130, 180), font=pill_font)
    draw.text((W - 250, 62), "Production Architecture", fill=(70, 95, 140), font=subtitle_font)

    # 4. Calculate Stage Boxes (5 stages horizontally)
    num_stages = len(stages)
    margin_x = 40
    avail_w = W - (2 * margin_x)
    box_w = int((avail_w - (num_stages - 1) * 35) / num_stages)
    box_y = 175
    box_h = 390

    for i, stage in enumerate(stages):
        bx = margin_x + i * (box_w + 35)
        by = box_y

        # Draw card container
        accent_color = stage.get("color", CARD_BORDER)
        draw.rounded_rectangle([bx, by, bx + box_w, by + box_h], radius=12, fill=CARD_BG, outline=accent_color, width=2)

        # Draw stage number pill
        num_x = bx + 16
        num_y = by + 16
        draw.ellipse([num_x, num_y, num_x + 28, num_y + 28], fill=STAGE_NUM_BG)
        draw.text((num_x + 9, num_y + 6), str(i + 1), fill=(255, 255, 255), font=pill_font)

        # Stage Category / Tag
        cat_text = stage.get("category", f"STAGE {i+1}").upper()
        draw.text((num_x + 36, num_y + 7), cat_text, fill=TEXT_ACCENT, font=pill_font)

        # Stage Title
        draw.text((bx + 16, by + 56), stage["title"], fill=TEXT_MAIN, font=box_header_font)

        # Separator line
        draw.line([bx + 16, by + 86, bx + box_w - 16, by + 86], fill=(35, 60, 110), width=1)

        # Bullet items / details
        curr_y = by + 100
        for item in stage["items"]:
            # Small bullet dot
            draw.ellipse([bx + 16, curr_y + 5, bx + 22, curr_y + 11], fill=(56, 189, 248))
            draw.text((bx + 28, curr_y), item, fill=TEXT_MUTED, font=box_body_font)
            curr_y += 34

        # Component footer tag inside box
        tech_badge = stage.get("tech", "")
        if tech_badge:
            draw.rounded_rectangle([bx + 14, by + box_h - 40, bx + box_w - 14, by + box_h - 14], radius=6, fill=(15, 23, 42), outline=(30, 58, 138), width=1)
            draw.text((bx + 22, by + box_h - 33), tech_badge, fill=TEXT_ACCENT, font=arrow_font)

        # Draw connecting arrow to next stage
        if i < num_stages - 1:
            arrow_start_x = bx + box_w + 3
            arrow_end_x = arrow_start_x + 28
            arrow_mid_y = by + 195
            draw.line([arrow_start_x, arrow_mid_y, arrow_end_x, arrow_mid_y], fill=ARROW_COLOR, width=3)
            # Arrow head
            draw.polygon([
                (arrow_end_x, arrow_mid_y),
                (arrow_end_x - 7, arrow_mid_y - 5),
                (arrow_end_x - 7, arrow_mid_y + 5)
            ], fill=ARROW_COLOR)

            # Arrow label if available
            if i < len(arrow_labels):
                label = arrow_labels[i]
                draw.text((arrow_start_x - 6, arrow_mid_y - 20), label, fill=(147, 197, 253), font=arrow_font)

    # 5. Bottom Footer Highlights
    draw.text((40, H - 40), footer_text, fill=TEXT_MUTED, font=footer_font)
    draw.text((W - 280, H - 40), "Status: Production Verified", fill=(52, 211, 153), font=footer_font)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    img.save(output_path)
    print(f"Generated: {output_path}")

# Configurations for all 14 solutions
DIAGRAMS = [
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/01_sqlserver_to_adls/architecture.png",
        "badge": "LAKEFORGE PATTERN #01",
        "title": "SQL Server to ADLS Gen2 High-Throughput Pipeline",
        "subtitle": "Partitioned Parallel JDBC Extraction with Watermarking into Bronze Delta Lake",
        "footer": "Tech: Azure SQL / SQL Server | PySpark | ADLS Gen2 | Delta Lake | Key Vault",
        "arrows": ["JDBC", "Secret", "Parquet", "Catalog"],
        "stages": [
            {
                "category": "Source DB",
                "title": "SQL Server OLTP",
                "color": (59, 130, 246),
                "items": [
                    "Sales.Orders table",
                    "Indexed OrderDate & ID",
                    "High-volume OLTP DB",
                    "Lower/Upper bound scan",
                    "Read-only replica"
                ],
                "tech": "Microsoft SQL / Azure SQL"
            },
            {
                "category": "Security",
                "title": "Azure Key Vault",
                "color": (168, 85, 247),
                "items": [
                    "Managed Identity Auth",
                    "dbutils secret scope",
                    "Zero hardcoded creds",
                    "SSL/TLS 1.3 transit",
                    "Dynamic token refresh"
                ],
                "tech": "Azure Key Vault / Secrets"
            },
            {
                "category": "Engine",
                "title": "LakeForge Ingest",
                "color": (14, 165, 233),
                "items": [
                    "16 parallel partitions",
                    "Pushdown predicates",
                    "Audit metadata cols",
                    "Schema auto-detect",
                    "Watermark detection"
                ],
                "tech": "PySpark / LakeForge Loader"
            },
            {
                "category": "Storage",
                "title": "ADLS Gen2 Bronze",
                "color": (34, 197, 94),
                "items": [
                    "Delta Lake format",
                    "Snappy compression",
                    "Date partition layout",
                    "ACID transaction log",
                    "Checkpoint management"
                ],
                "tech": "ADLS Gen2 / Delta Lake"
            },
            {
                "category": "Observability",
                "title": "Audit & Watermark",
                "color": (234, 179, 8),
                "items": [
                    "Ingestion audit log",
                    "Row count validation",
                    "State store persistence",
                    "Slack/Teams alerts",
                    "Downstream trigger"
                ],
                "tech": "Unity Catalog / State Store"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/02_rest_api_to_delta/architecture.png",
        "badge": "LAKEFORGE PATTERN #02",
        "title": "Paginated REST API to Delta Lake Ingestion",
        "subtitle": "Token Refresh, Exponential Backoff, Rate Throttling & Dynamic Schema Evolution",
        "footer": "Tech: REST SaaS APIs | OAuth2 | PySpark | Schema Evolution | Delta Lake",
        "arrows": ["OAuth", "Paging", "Parse", "Append"],
        "stages": [
            {
                "category": "SaaS Source",
                "title": "External REST API",
                "color": (59, 130, 246),
                "items": [
                    "Stripe / Salesforce API",
                    "Paginated JSON data",
                    "Strict rate limits (429)",
                    "Token expiration rules",
                    "Nested JSON payloads"
                ],
                "tech": "SaaS REST / HTTP Endpoints"
            },
            {
                "category": "Auth & Resiliency",
                "title": "LakeForge API Client",
                "color": (168, 85, 247),
                "items": [
                    "Automatic token refresh",
                    "Exponential backoff retry",
                    "Rate limiter (token bucket)",
                    "Cursor / offset paging",
                    "Stream payload buffer"
                ],
                "tech": "LakeForge REST Connector"
            },
            {
                "category": "Processing",
                "title": "PySpark Normalizer",
                "color": (14, 165, 233),
                "items": [
                    "Schema auto-detection",
                    "Nested struct flattening",
                    "Data quality validation",
                    "Corrupt row quarantine",
                    "Audit metadata injector"
                ],
                "tech": "PySpark / LakeForge DQ"
            },
            {
                "category": "Storage",
                "title": "Delta Bronze Table",
                "color": (34, 197, 94),
                "items": [
                    "Schema evolution enabled",
                    "mergeSchema = true",
                    "Idempotent ingestion",
                    "Delta time-travel audit",
                    "Clean raw history"
                ],
                "tech": "Delta Lake / Unity Catalog"
            },
            {
                "category": "Telemetry",
                "title": "API Observability",
                "color": (234, 179, 8),
                "items": [
                    "API quota consumption",
                    "Payload latency metrics",
                    "Quarantined JSON store",
                    "Failure event logs",
                    "Downstream notifications"
                ],
                "tech": "LakeForge Observability"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/03_adf_metadata_framework/architecture.png",
        "badge": "LAKEFORGE PATTERN #03",
        "title": "Metadata-Driven Ingestion Pipeline (ADF + Databricks)",
        "subtitle": "Single Reusable ADF Pipeline Orchestrating 100+ Tables via SQL Control Table",
        "footer": "Tech: Azure Data Factory | Azure SQL Control DB | Databricks Job API | Delta Lake",
        "arrows": ["Lookup", "ForEach", "Dispatch", "Log State"],
        "stages": [
            {
                "category": "Control Store",
                "title": "SQL Control Table",
                "color": (59, 130, 246),
                "items": [
                    "Source/Target configs",
                    "Watermark columns",
                    "Active status flags",
                    "Execution priority",
                    "SLA & Schedule cadences"
                ],
                "tech": "Azure SQL / Metadata DB"
            },
            {
                "category": "Orchestrator",
                "title": "Azure Data Factory",
                "color": (14, 165, 233),
                "items": [
                    "Lookup active tables",
                    "Filter by execution batch",
                    "ForEach concurrency (20)",
                    "Databricks Notebook activity",
                    "Error handler & alerts"
                ],
                "tech": "Azure Data Factory v2"
            },
            {
                "category": "Executor",
                "title": "LakeForge Runner",
                "color": (168, 85, 247),
                "items": [
                    "Generic parameter parser",
                    "Dynamic table loader",
                    "Auto-adjusting cluster",
                    "Idempotent execution",
                    "Telemetry hook callback"
                ],
                "tech": "Databricks Dynamic Worker"
            },
            {
                "category": "Lakehouse",
                "title": "ADLS Gen2 Storage",
                "color": (34, 197, 94),
                "items": [
                    "Automated Bronze landing",
                    "Delta ACID guarantees",
                    "Standard directory paths",
                    "Daily partition schemes",
                    "Auto Loader discovery"
                ],
                "tech": "ADLS Gen2 / Delta Lake"
            },
            {
                "category": "Audit & Ops",
                "title": "Execution Logging",
                "color": (234, 179, 8),
                "items": [
                    "Pipeline run log table",
                    "Row count delta metrics",
                    "Execution duration track",
                    "Watermark advancement",
                    "Power BI ops dashboard"
                ],
                "tech": "Audit Table / Log Analytics"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/04_bronze_silver_gold/architecture.png",
        "badge": "LAKEFORGE PATTERN #04",
        "title": "Enterprise Medallion Architecture (Bronze -> Silver -> Gold)",
        "subtitle": "Multi-Hop Pipeline with Schema Gates, Data Quality Quarantining & Star Schema Serving",
        "footer": "Tech: Databricks | LakeForge DQ | Delta Lake | Kimball Star Schema | Power BI",
        "arrows": ["Raw Landing", "Clean & Gate", "Aggregate", "Serve BI"],
        "stages": [
            {
                "category": "Source Layer",
                "title": "Raw Data Influx",
                "color": (59, 130, 246),
                "items": [
                    "OLTP DB tables",
                    "Clickstream events",
                    "SaaS CRM exports",
                    "Batch file drops",
                    "Streaming Kafka topics"
                ],
                "tech": "Multi-Source Connectors"
            },
            {
                "category": "Bronze Layer",
                "title": "Bronze (Raw Immutable)",
                "color": (180, 83, 9),
                "items": [
                    "100% faithful raw state",
                    "Audit metadata attached",
                    "_ingest_timestamp column",
                    "_source_file origin",
                    "Schema on read fallback"
                ],
                "tech": "Delta Lake Bronze"
            },
            {
                "category": "Silver Layer",
                "title": "Silver (Conformed)",
                "color": (148, 163, 184),
                "items": [
                    "LakeForge DQ validation",
                    "Deduplication on PK",
                    "Type casting & clean",
                    "Quarantine table route",
                    "SCD1 / SCD2 dimensions"
                ],
                "tech": "LakeForge Silver Engine"
            },
            {
                "category": "Gold Layer",
                "title": "Gold (Business Marts)",
                "color": (234, 179, 8),
                "items": [
                    "Kimball dimensional model",
                    "Fact_Sales & Dim_Customer",
                    "Pre-calculated KPIs",
                    "Optimized Z-Order layout",
                    "Sub-second aggregations"
                ],
                "tech": "Delta Lake Gold Marts"
            },
            {
                "category": "Consumption",
                "title": "Serving Layer",
                "color": (34, 197, 94),
                "items": [
                    "Power BI Direct Lake",
                    "Databricks SQL Warehouse",
                    "ML Feature Store",
                    "External data sharing",
                    "Zero-copy queries"
                ],
                "tech": "Unity Catalog / Direct Lake"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/05_scd_type2/architecture.png",
        "badge": "LAKEFORGE PATTERN #05",
        "title": "Automated Slowly Changing Dimensions (SCD Type 2)",
        "subtitle": "Complete History Tracking with Delta MERGE, Surrogate Keys & Effective Dates",
        "footer": "Tech: PySpark | Delta Lake MERGE | LakeForge SCD Engine | Unity Catalog",
        "arrows": ["Staging", "Hash Diff", "Single MERGE", "Analytics"],
        "stages": [
            {
                "category": "Input Stream",
                "title": "CDC / Delta Staging",
                "color": (59, 130, 246),
                "items": [
                    "Customer updates feed",
                    "Natural Key: customer_id",
                    "Tracked attrs: address, tier",
                    "Event change timestamp",
                    "Deduplicated staging set"
                ],
                "tech": "Staging DataFrame"
            },
            {
                "category": "Diff Engine",
                "title": "LakeForge SCD2",
                "color": (14, 165, 233),
                "items": [
                    "SHA256 attribute hash",
                    "Detect actual mutations",
                    "Generate surrogate keys",
                    "Split: updates vs new",
                    "Prepare merge staging"
                ],
                "tech": "LakeForge SCD2 Engine"
            },
            {
                "category": "Delta Merge",
                "title": "Delta MERGE Operation",
                "color": (168, 85, 247),
                "items": [
                    "Atomic single-pass merge",
                    "Close existing active row",
                    "Set valid_to = event_time",
                    "Set is_current = 'N'",
                    "Insert new current record"
                ],
                "tech": "Delta Lake ACID Merge"
            },
            {
                "category": "Target Dimension",
                "title": "Dim_Customer Delta",
                "color": (34, 197, 94),
                "items": [
                    "surrogate_key (SK)",
                    "valid_from & valid_to",
                    "is_current ('Y' / 'N')",
                    "Full point-in-time audit",
                    "Z-Ordered by customer_id"
                ],
                "tech": "Delta Lake Dimension"
            },
            {
                "category": "Query Layer",
                "title": "Time-Travel Analytics",
                "color": (234, 179, 8),
                "items": [
                    "As-of date join logic",
                    "Historical revenue attribution",
                    "Regulatory GDPR audit",
                    "ML model backtesting",
                    "Zero data overwrite"
                ],
                "tech": "Databricks SQL / Time-Travel"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/06_incremental_load/architecture.png",
        "badge": "LAKEFORGE PATTERN #06",
        "title": "High-Performance Watermark Incremental Load",
        "subtitle": "Stateful Watermark Tracking with Delta MERGE for High-Volume Ingestion",
        "footer": "Tech: PySpark | LakeForge State Store | Delta Lake MERGE | Idempotent Upsert",
        "arrows": ["Read Watermark", "Extract Delta", "Upsert Target", "Commit State"],
        "stages": [
            {
                "category": "Source Table",
                "title": "OLTP Database",
                "color": (59, 130, 246),
                "items": [
                    "Large orders table (100M+)",
                    "ModifiedDate index",
                    "Batch extraction window",
                    "Continuous updates/inserts",
                    "Low impact read replica"
                ],
                "tech": "Relational DB / ERP"
            },
            {
                "category": "State Store",
                "title": "LakeForge Watermark",
                "color": (168, 85, 247),
                "items": [
                    "Delta watermark registry",
                    "Fetch last_successful_ts",
                    "Handle safety buffer / lag",
                    "Audit past run history",
                    "Fault-tolerant recovery"
                ],
                "tech": "Watermark State Store"
            },
            {
                "category": "Incremental Scan",
                "title": "LakeForge Ingestion",
                "color": (14, 165, 233),
                "items": [
                    "Filtered pushdown query",
                    "WHERE ModifiedDate > ts",
                    "Extract only delta batch",
                    "Calculate new max_ts",
                    "Data quality validation"
                ],
                "tech": "PySpark Delta Extractor"
            },
            {
                "category": "Target Upsert",
                "title": "Delta Lake Upsert",
                "color": (34, 197, 94),
                "items": [
                    "Delta MERGE ON order_id",
                    "WHEN MATCHED UPDATE",
                    "WHEN NOT MATCHED INSERT",
                    "Zero duplicate records",
                    "Idempotent pipeline rerun"
                ],
                "tech": "Delta Lake Silver Table"
            },
            {
                "category": "Commit & Notify",
                "title": "State Store Commit",
                "color": (234, 179, 8),
                "items": [
                    "Update state store record",
                    "Atomic watermark commit",
                    "Log row counts & timings",
                    "Lineage event published",
                    "Downstream event trigger"
                ],
                "tech": "LakeForge Governance"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/07_cdc_pipeline/architecture.png",
        "badge": "LAKEFORGE PATTERN #07",
        "title": "End-to-End Real-Time Change Data Capture (CDC)",
        "subtitle": "Debezium -> Kafka -> Spark Structured Streaming -> Delta CDF Replication",
        "footer": "Tech: PostgreSQL | Debezium | Apache Kafka | Structured Streaming | Delta CDF",
        "arrows": ["WAL Stream", "Events", "Micro-batch", "Replicate"],
        "stages": [
            {
                "category": "Source DB",
                "title": "PostgreSQL WAL",
                "color": (59, 130, 246),
                "items": [
                    "Write-Ahead Log (WAL)",
                    "Row-level changes",
                    "Zero query load on DB",
                    "Captures inserts/updates",
                    "Captures hard deletes"
                ],
                "tech": "PostgreSQL / MySQL WAL"
            },
            {
                "category": "CDC Hub",
                "title": "Debezium & Kafka",
                "color": (14, 165, 233),
                "items": [
                    "Debezium connector",
                    "JSON / Avro schema registry",
                    "Kafka partitioned topics",
                    "Op codes: c, u, d, r",
                    "Before & After state payload"
                ],
                "tech": "Debezium / Apache Kafka"
            },
            {
                "category": "Stream Engine",
                "title": "Spark Streaming",
                "color": (168, 85, 247),
                "items": [
                    "Structured Streaming",
                    "Micro-batch (10 sec)",
                    "Debezium parser engine",
                    "Deduplicate by txn LSN",
                    "Checkpoint management"
                ],
                "tech": "LakeForge Stream Parser"
            },
            {
                "category": "Replicated Target",
                "title": "Delta Lake Silver",
                "color": (34, 197, 94),
                "items": [
                    "Delta MERGE implementation",
                    "Delete rows on Op='d'",
                    "Upsert on Op in ('c', 'u')",
                    "Exactly-once delivery",
                    "Consistent replica state"
                ],
                "tech": "Delta Lake Replicated Store"
            },
            {
                "category": "Change Feed",
                "title": "Delta CDF (Change Feed)",
                "color": (234, 179, 8),
                "items": [
                    "Change Data Feed enabled",
                    "Downstream consumers",
                    "Audit event tracking",
                    "Real-time cache invalidation",
                    "Microservices broadcast"
                ],
                "tech": "Delta Change Data Feed"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/08_unity_catalog/architecture.png",
        "badge": "LAKEFORGE PATTERN #08",
        "title": "Unity Catalog Enterprise Governance & Security",
        "subtitle": "3-Level Namespace, Dynamic Row-Level Security, Column Masking & Lineage",
        "footer": "Tech: Unity Catalog | Entra ID | Dynamic Masking | Row Filters | Lineage Graph",
        "arrows": ["Namespace", "RBAC Grant", "Mask Policy", "Audit Track"],
        "stages": [
            {
                "category": "Central Metastore",
                "title": "Unity Catalog Metastore",
                "color": (59, 130, 246),
                "items": [
                    "Single unified metastore",
                    "Cross-workspace governance",
                    "Cloud storage credentials",
                    "Managed / External tables",
                    "Volume storage management"
                ],
                "tech": "Unity Catalog Metastore"
            },
            {
                "category": "Namespace",
                "title": "3-Level Hierarchy",
                "color": (14, 165, 233),
                "items": [
                    "prod_catalog",
                    "healthcare_silver schema",
                    "patients_master table",
                    "Clear logical separation",
                    "Tag-based classification"
                ],
                "tech": "Catalog . Schema . Table"
            },
            {
                "category": "Identity & RBAC",
                "title": "Entra ID & Grants",
                "color": (168, 85, 247),
                "items": [
                    "Azure AD / Entra ID sync",
                    "Group-based permissions",
                    "GRANT SELECT ON TABLE",
                    "Data Engineers: MODIFY",
                    "Analysts: Read-Only"
                ],
                "tech": "Entra ID / UC RBAC"
            },
            {
                "category": "Data Protection",
                "title": "Masking & Filtering",
                "color": (234, 179, 8),
                "items": [
                    "Dynamic column masking",
                    "Mask SSN / Credit Cards",
                    "Row filter by region",
                    "IS_ACCOUNT_GROUP_MEMBER",
                    "Zero ETL view duplication"
                ],
                "tech": "Column Mask & Row Filter"
            },
            {
                "category": "Lineage & Audit",
                "title": "Lineage & Audit Logs",
                "color": (34, 197, 94),
                "items": [
                    "Column-level lineage graph",
                    "Track upstream to dashboard",
                    "System audit tables",
                    "HIPAA & GDPR compliance",
                    "Automated security scans"
                ],
                "tech": "UC Lineage & System Tables"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/09_delta_live_tables/architecture.png",
        "badge": "LAKEFORGE PATTERN #09",
        "title": "Declarative Delta Live Tables (DLT) Pipeline",
        "subtitle": "Auto Loader Streaming Ingestion with DLT Expectations & Automated DAG Execution",
        "footer": "Tech: Delta Live Tables | Auto Loader | Data Expectations | Spark Streaming | Unity Catalog",
        "arrows": ["Auto Loader", "Stream Ingest", "Expectations", "Materialize"],
        "stages": [
            {
                "category": "Data Drops",
                "title": "Cloud Object Store",
                "color": (59, 130, 246),
                "items": [
                    "ADLS Gen2 / S3 bucket",
                    "IoT sensor telemetry files",
                    "JSON / Parquet continuous",
                    "Directory partitioning",
                    "Multi-tenant file landing"
                ],
                "tech": "ADLS Gen2 / S3"
            },
            {
                "category": "Auto Ingestion",
                "title": "Auto Loader (cloudFiles)",
                "color": (14, 165, 233),
                "items": [
                    "Schema inference & evolution",
                    "File notification mode",
                    "Automatic incremental ingest",
                    "Zero file listing bottlenecks",
                    "Handles millions of files"
                ],
                "tech": "Databricks Auto Loader"
            },
            {
                "category": "DLT Bronze",
                "title": "@dlt.table Bronze",
                "color": (180, 83, 9),
                "items": [
                    "Declarative Python syntax",
                    "Streaming live table",
                    "Raw JSON ingestion",
                    "Metadata tracking",
                    "Automatic checkpointing"
                ],
                "tech": "DLT Streaming Table"
            },
            {
                "category": "DLT Silver",
                "title": "@dlt.expect Silver",
                "color": (148, 163, 184),
                "items": [
                    "@dlt.expect_or_drop",
                    "Enforce valid temperature",
                    "@dlt.expect_or_fail",
                    "Null device_id check",
                    "Auto quality metric event log"
                ],
                "tech": "DLT Quality Expectations"
            },
            {
                "category": "DLT Gold",
                "title": "@dlt.table Gold Mart",
                "color": (234, 179, 8),
                "items": [
                    "Device hourly aggregations",
                    "Min/Max/Avg metrics",
                    "Materialized view refresh",
                    "Lineage graph visualization",
                    "Auto-healing & scaling"
                ],
                "tech": "DLT Gold / Materialized View"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/10_real_time_pipeline/architecture.png",
        "badge": "LAKEFORGE PATTERN #10",
        "title": "Real-Time Streaming & Fraud Detection Pipeline",
        "subtitle": "Spark Structured Streaming with Sliding Windows, Watermarks & Alerting",
        "footer": "Tech: Apache Kafka | Spark Structured Streaming | Watermarking | Delta Lake | Real-Time",
        "arrows": ["Produce", "Stream Consume", "Sliding Window", "Alert Push"],
        "stages": [
            {
                "category": "Event Source",
                "title": "Event Producer",
                "color": (59, 130, 246),
                "items": [
                    "Card transactions & clicks",
                    "10,000 events / sec",
                    "JSON payload with timestamp",
                    "Cardholder ID & amount",
                    "Geographic coordinates"
                ],
                "tech": "Apache Kafka / Event Hubs"
            },
            {
                "category": "Streaming Cluster",
                "title": "Structured Streaming",
                "color": (14, 165, 233),
                "items": [
                    "Micro-batch trigger (1s)",
                    "Watermark 10 min late data",
                    "Drop out-of-order anomalies",
                    "Failover checkpoint store",
                    "Exactly-once semantics"
                ],
                "tech": "Spark Streaming Engine"
            },
            {
                "category": "Analytics",
                "title": "Sliding Window Logic",
                "color": (168, 85, 247),
                "items": [
                    "10-min window, 2-min slide",
                    "Count txns per cardholder",
                    "Velocity spike detection",
                    "Rule: >3 txns in 10 mins",
                    "Calculates fraud risk score"
                ],
                "tech": "LakeForge Fraud Engine"
            },
            {
                "category": "Delta Storage",
                "title": "Delta Lake Streaming",
                "color": (34, 197, 94),
                "items": [
                    "Bronze raw events append",
                    "Silver enriched aggregations",
                    "ACID consistency check",
                    "Compact small files",
                    "Partitioned by event date"
                ],
                "tech": "Delta Lake Streaming Sink"
            },
            {
                "category": "Alert Dispatch",
                "title": "Fraud Alerts & BI",
                "color": (239, 68, 68),
                "items": [
                    "real_time_fraud_alerts table",
                    "Instant security webhook",
                    "Card lock trigger API",
                    "SOC live dashboard",
                    "Sub-5-second SLA"
                ],
                "tech": "Real-Time Alert Dispatcher"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/11_fabric_pipeline/architecture.png",
        "badge": "LAKEFORGE PATTERN #11",
        "title": "Microsoft Fabric Lakehouse & OneLake Pipeline",
        "subtitle": "Direct Lake Storage Shortcutting, Fabric Spark & Delta Parquet Interoperability",
        "footer": "Tech: Microsoft Fabric | OneLake | Fabric Data Factory | Fabric Spark | Direct Lake",
        "arrows": ["Ingest", "OneLake Store", "Spark Medallion", "Direct Lake"],
        "stages": [
            {
                "category": "Hybrid Influx",
                "title": "Hybrid Data Sources",
                "color": (59, 130, 246),
                "items": [
                    "Enterprise ERP / SAP",
                    "Azure SQL Databases",
                    "Databricks Lakehouse",
                    "Cloud file shares",
                    "REST API services"
                ],
                "tech": "Fabric Connectors"
            },
            {
                "category": "Unified Store",
                "title": "Fabric OneLake",
                "color": (14, 165, 233),
                "items": [
                    "Single unified SaaS lake",
                    "Zero-copy shortcuts",
                    "Eliminate data duplication",
                    "Delta Parquet native",
                    "Workspace security"
                ],
                "tech": "Microsoft OneLake"
            },
            {
                "category": "Orchestrator",
                "title": "Fabric Data Factory",
                "color": (168, 85, 247),
                "items": [
                    "Cloud-scale data pipelines",
                    "Dataflows Gen2 low-code",
                    "Fast copy activities",
                    "Scheduled trigger & monitors",
                    "Fabric REST API hooks"
                ],
                "tech": "Fabric Data Pipeline"
            },
            {
                "category": "Compute Engine",
                "title": "Fabric Synapse Spark",
                "color": (34, 197, 94),
                "items": [
                    "LakeForge Spark notebooks",
                    "Medallion transforms",
                    "V-Order optimized Parquet",
                    "High-concurrency pool",
                    "Delta ACID transactions"
                ],
                "tech": "Fabric PySpark Engine"
            },
            {
                "category": "Serving Layer",
                "title": "Direct Lake & Power BI",
                "color": (234, 179, 8),
                "items": [
                    "Direct Lake mode",
                    "Zero import query latency",
                    "Direct querying on OneLake",
                    "Copilot AI assistance",
                    "Enterprise dashboards"
                ],
                "tech": "Power BI / Fabric Copilot"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/12_powerbi_dashboard/architecture.png",
        "badge": "LAKEFORGE PATTERN #12",
        "title": "Direct Lake Power BI Semantic Model & Dashboard",
        "subtitle": "Sub-Second Queries on Terabyte Delta Tables with Zero Import Delays",
        "footer": "Tech: Power BI | Direct Lake | Databricks SQL | Star Schema | Advanced DAX",
        "arrows": ["Gold Store", "Performance", "Direct Connect", "Executive BI"],
        "stages": [
            {
                "category": "Gold Lakehouse",
                "title": "Gold Delta Tables",
                "color": (234, 179, 8),
                "items": [
                    "fact_sales (100M+ rows)",
                    "dim_customer (SCD2)",
                    "dim_product & category",
                    "dim_date (Role-playing)",
                    "dim_store (Geo entity)"
                ],
                "tech": "Delta Lake Gold Layer"
            },
            {
                "category": "Delta Tuning",
                "title": "LakeForge Tuning",
                "color": (14, 165, 233),
                "items": [
                    "Liquid Clustering by Date/Key",
                    "Pre-computed statistics",
                    "OPTIMIZE file compaction",
                    "V-Order Parquet sorting",
                    "Instant columnar pruning"
                ],
                "tech": "Delta Performance Engine"
            },
            {
                "category": "Semantic Layer",
                "title": "Direct Lake Model",
                "color": (168, 85, 247),
                "items": [
                    "Direct Lake connection",
                    "Bypasses Analysis Services import",
                    "Zero ETL latency / refreshes",
                    "Star schema 1:M relations",
                    "TMDL definition format"
                ],
                "tech": "Power BI Semantic Model"
            },
            {
                "category": "Calculations",
                "title": "Advanced DAX Engine",
                "color": (59, 130, 246),
                "items": [
                    "Total Revenue & Margin",
                    "YoY / MoM Growth measures",
                    "Customer LTV calculations",
                    "Dynamic currency conversion",
                    "RLS security filtering"
                ],
                "tech": "DAX Business Measures"
            },
            {
                "category": "Executive BI",
                "title": "Interactive Dashboards",
                "color": (34, 197, 94),
                "items": [
                    "Executive KPI summary",
                    "Interactive cross-filtering",
                    "Geographic sales map",
                    "Drill-through detail view",
                    "Mobile & Teams embedded"
                ],
                "tech": "Power BI Service / Apps"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/13_data_quality_trust_engine/architecture.png",
        "badge": "LAKEFORGE PATTERN #13",
        "title": "Automated Data Quality & Trust Engine",
        "subtitle": "Declarative Profiling, Anomaly Quarantine & Quality Scorecard Reporting",
        "footer": "Tech: LakeForge Trust Engine | PySpark | Delta Lake Quarantine | Slack/Teams Alerts",
        "arrows": ["Raw Data", "Rules Gate", "Quarantine/Pass", "Scorecard"],
        "stages": [
            {
                "category": "Incoming Batch",
                "title": "Bronze Landing Data",
                "color": (59, 130, 246),
                "items": [
                    "Raw ingested records",
                    "Vendor data feeds",
                    "Missing values & anomalies",
                    "Schema drift & typos",
                    "Duplicate transactions"
                ],
                "tech": "Delta Bronze Table"
            },
            {
                "category": "Rules Engine",
                "title": "LakeForge Rules Gate",
                "color": (14, 165, 233),
                "items": [
                    "Completeness (Null checks)",
                    "Uniqueness (Duplicate check)",
                    "Validity (Regex email/phone)",
                    "Accuracy (Range checks)",
                    "Referential integrity tests"
                ],
                "tech": "LakeForge Trust Engine"
            },
            {
                "category": "Gatekeeper",
                "title": "Split Routing Logic",
                "color": (168, 85, 247),
                "items": [
                    "Conditional DataFrame split",
                    "Zero dirty data leakage",
                    "Tag records with failed rules",
                    "Compute row-level score",
                    "Preserve full raw context"
                ],
                "tech": "LakeForge Validation Router"
            },
            {
                "category": "Target Stores",
                "title": "Silver & Quarantine",
                "color": (34, 197, 94),
                "items": [
                    "Silver table (100% clean)",
                    "Quarantine table (rejected)",
                    "Failure reason column",
                    "Remediation workflow",
                    "Re-ingestion support"
                ],
                "tech": "Delta Clean & Quarantine"
            },
            {
                "category": "Monitoring",
                "title": "DQ Scorecard & Alerts",
                "color": (234, 179, 8),
                "items": [
                    "Table trust score (0-100%)",
                    "Automated Slack alert",
                    "Data quality trend report",
                    "Pipeline circuit breaker",
                    "Compliance audit history"
                ],
                "tech": "LakeForge Scorecard Engine"
            }
        ]
    },
    {
        "path": "d:/ADT & Databricks/lakeforge-examples/examples/14_delta_lake_optimization/architecture.png",
        "badge": "LAKEFORGE PATTERN #14",
        "title": "Delta Lake Performance & Maintenance Engine",
        "subtitle": "Liquid Clustering, Compaction (OPTIMIZE), VACUUM & Storage Cost Reduction",
        "footer": "Tech: Delta Lake | Liquid Clustering | OPTIMIZE | VACUUM | LakeForge Maintenance",
        "arrows": ["Fragmented", "Clustering", "Compaction", "Clean Vacuum"],
        "stages": [
            {
                "category": "Table Health",
                "title": "Fragmented Delta Table",
                "color": (239, 68, 68),
                "items": [
                    "Small file problem (<10MB)",
                    "Frequent micro-batch writes",
                    "Outdated transaction logs",
                    "Slow scan / query times",
                    "Bloated storage bill"
                ],
                "tech": "Delta Lake Active Table"
            },
            {
                "category": "Clustering",
                "title": "Liquid Clustering",
                "color": (14, 165, 233),
                "items": [
                    "CLUSTER BY (keys...)",
                    "Replaces rigid partitions",
                    "No partition skew or drift",
                    "Dynamic layout evolution",
                    "Multi-dimensional pruning"
                ],
                "tech": "Delta Liquid Clustering"
            },
            {
                "category": "Compaction",
                "title": "OPTIMIZE Bin-Packing",
                "color": (168, 85, 247),
                "items": [
                    "OPTIMIZE table command",
                    "Coalesces small files",
                    "Target 128MB - 1GB files",
                    "Z-Order indexing support",
                    "Scheduled off-peak cron"
                ],
                "tech": "LakeForge Compactor"
            },
            {
                "category": "Cleanup",
                "title": "VACUUM Garbage Clean",
                "color": (234, 179, 8),
                "items": [
                    "VACUUM table RETAIN 168h",
                    "Remove unreferenced files",
                    "Preserves time-travel safety",
                    "Dry-run safety mode",
                    "Reclaims terabytes of cloud disk"
                ],
                "tech": "Delta VACUUM Engine"
            },
            {
                "category": "Business Impact",
                "title": "10x Performance Boost",
                "color": (34, 197, 94),
                "items": [
                    "10x faster query response",
                    "50%+ storage cost savings",
                    "Instant metadata parsing",
                    "Predictable dashboard SLA",
                    "Automated self-healing tables"
                ],
                "tech": "Optimized Lakehouse"
            }
        ]
    }
]

if __name__ == "__main__":
    for diag in DIAGRAMS:
        draw_diagram(
            output_path=diag["path"],
            badge_text=diag["badge"],
            title=diag["title"],
            subtitle=diag["subtitle"],
            stages=diag["stages"],
            arrow_labels=diag["arrows"],
            footer_text=diag["footer"]
        )
    print("All 14 architecture diagrams generated successfully!")
