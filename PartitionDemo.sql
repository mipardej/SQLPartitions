-- Step 1: Create the Partition Function
-- Partition function divides data into ranges based on the datetime column.
CREATE PARTITION FUNCTION DateTimePartitionFunction (DATETIME)
AS RANGE RIGHT FOR VALUES
    ('2023-01-01T00:00:00',  -- Partition 1: data before or equal to this date
     '2024-01-01T00:00:00',  -- Partition 2: data between this date and the next
     '2025-01-01T00:00:00'); -- Partition 3: data after this date

-- Step 2: Create the Partition Schema
-- The schema specifies the filegroups where each partition will be stored.
CREATE PARTITION SCHEME DateTimePartitionScheme
AS PARTITION DateTimePartitionFunction
TO ([PRIMARY],[FG2022], [FG2023], [FG2024]); -- Filegroups where the partitions will be placed

-- Step 3: Create the Table
-- The table will have multiple columns, one of which is a DATETIME column.
CREATE TABLE SalesData (
    ID INT IDENTITY,   -- ID Key for the table
    SalesAmount DECIMAL(18, 2) NOT NULL,    -- Example column for sales amount
    SalesDate DATETIME NOT NULL,			-- The DATETIME column for partitioning
    CustomerID INT,							-- Example column for customer ID
    Region NVARCHAR(50)						-- Example column for region
)
ON DateTimePartitionScheme(SalesDate); -- Assign the partition scheme based on SalesDate column

-- Step 4: Create Clustered Columnstore Index on the Table
-- A clustered columnstore index is efficient for large-scale data warehouses.
CREATE CLUSTERED COLUMNSTORE INDEX CCI_SalesData
ON SalesData 


-- Example of adding a row of data to test the partitioning and indexing
INSERT INTO SalesData (SalesAmount, SalesDate, CustomerID, Region) VALUES (120.50, '2022-11-01', 102, 'South');
INSERT INTO SalesData (SalesAmount, SalesDate, CustomerID, Region) VALUES (100.99, '2023-11-01', 90, 'South');
INSERT INTO SalesData (SalesAmount, SalesDate, CustomerID, Region) VALUES (510.50, '2024-11-01', 62, 'South');

-- Optional: Query to check partitioning
SELECT $PARTITION.DateTimePartitionFunction(SalesDate) AS PartitionID, *
FROM SalesData
WHERE SalesDate = '2023-05-01';

-- Query to show the number of rows in each partition of the SalesData table





USE [master]
GO
ALTER DATABASE [PartitionDemo1] ADD FILE ( NAME = N'PartitionDemo1_2024', FILENAME = N'/var/opt/mssql/data/PartitionDemo1_2024.ndf' , SIZE = 8192KB , FILEGROWTH = 65536KB ) TO FILEGROUP [FG2024]
GO
ALTER DATABASE [PartitionDemo1] ADD FILE ( NAME = N'PartitionDemo1_2025', FILENAME = N'/var/opt/mssql/data/PartitionDemo1_2025.ndf' , SIZE = 8192KB , FILEGROWTH = 65536KB ) TO FILEGROUP [FG2025]
GO

SELECT $PARTITION.DateTimePartitionFunction(SalesDate) AS PartitionID, *
FROM SalesData
-- Query to show the number of rows in each partition of the SalesData table
SELECT 
    p.partition_number,               -- The partition number
    fg.name AS filegroup_name,        -- The filegroup name associated with the partition
    ps.row_count AS row_count         -- The number of rows in the partition
FROM 
    sys.partitions p
JOIN 
    sys.allocation_units au ON p.hobt_id = au.container_id
JOIN 
    sys.filegroups fg ON au.data_space_id = fg.data_space_id
JOIN 
    sys.dm_db_partition_stats ps ON p.partition_id = ps.partition_id
WHERE 
    p.object_id = OBJECT_ID('SalesData') -- Specify the table
    AND p.index_id <= 1               -- Exclude non-clustered indexes (index_id 0 = heap, index_id 1 = clustered index)
GROUP BY 
    p.partition_number, fg.name, ps.row_count
ORDER BY 
    p.partition_number;


select * from sys.filegroups