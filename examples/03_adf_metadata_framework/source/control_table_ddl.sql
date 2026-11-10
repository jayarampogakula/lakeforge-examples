-- DDL for LakeForge Metadata Control Table Architecture
CREATE SCHEMA etl;
GO

CREATE TABLE etl.ControlTable (
    PipelineID INT IDENTITY(1, 1) PRIMARY KEY,
    SourceSystem NVARCHAR(50) NOT NULL,
    SourceSchema NVARCHAR(50) NOT NULL,
    SourceTable NVARCHAR(100) NOT NULL,
    TargetContainer NVARCHAR(50) NOT NULL,
    TargetDirectory NVARCHAR(150) NOT NULL,
    WatermarkColumn NVARCHAR(50) NULL,
    LastWatermarkValue NVARCHAR(50) NULL,
    ExecutionCadence NVARCHAR(20) DEFAULT 'DAILY', -- HOURLY, DAILY, WEEKLY
    BatchGroup INT DEFAULT 1,
    IsActive BIT DEFAULT 1,
    LastRunStatus NVARCHAR(20) DEFAULT 'PENDING',
    LastRunDateTime DATETIME2 NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);

CREATE TABLE etl.ExecutionLogs (
    LogID BIGINT IDENTITY(1, 1) PRIMARY KEY,
    PipelineID INT NOT NULL FOREIGN KEY REFERENCES etl.ControlTable(PipelineID),
    ADFRunID NVARCHAR(100) NOT NULL,
    DatabricksJobID NVARCHAR(100) NULL,
    RowsRead BIGINT NULL,
    RowsWritten BIGINT NULL,
    ExecutionStatus NVARCHAR(20) NOT NULL, -- SUCCESS, FAILED, RUNNING
    ErrorMessage NVARCHAR(MAX) NULL,
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2 NULL,
    DurationSeconds DECIMAL(10, 2) NULL
);
