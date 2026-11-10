-- SQL Server DDL & Seed Script for Source Sales.Orders
CREATE SCHEMA Sales;
GO

CREATE TABLE Sales.Orders (
    OrderID INT IDENTITY(100001, 1) PRIMARY KEY,
    CustomerID NVARCHAR(20) NOT NULL,
    OrderDate DATE NOT NULL,
    OrderAmount DECIMAL(18, 2) NOT NULL,
    Currency VARCHAR(3) DEFAULT 'USD',
    OrderStatus VARCHAR(20) NOT NULL,
    Region VARCHAR(30) NOT NULL,
    LastModifiedDateTime DATETIME2 DEFAULT SYSUTCDATETIME()
);

-- Index to optimize partitioned reads and incremental watermarking
CREATE NONCLUSTERED INDEX IX_Orders_OrderDate_ID 
ON Sales.Orders(OrderDate, OrderID) 
INCLUDE (OrderAmount, OrderStatus);
