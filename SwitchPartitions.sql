SET NOCOUNT ON

DROP TABLE IF EXISTS #CSI_Switch 
DROP TABLE IF EXISTS #Indexes

DECLARE @SplitDate datetime = '2025-01-01 00:00:00'
DECLARE @PartitionSizeMonths int = 12

SELECT
schema_name(o.schema_id) as SchemaName,
object_name(i.object_id) as TableName,
i. [name] AS IndexName,
'select top 0 * into '+schema_name(o.schema_id)+'. '+object_name (i.object_id)+'_temp from '+schema_name (o.schema_id)+'.'+object_name(i.object_id) as CreateCopyTable,
'ALTER TABLE '+schema_name(o.schema_id)+'.'+object_name(i.object_id)+'_temp ADD CONSTRAINT CONS_'+object_name (i.object_id)+'_temp PRIMARY KEY ('+Col.PartitioningColumn+')
on '+p.file_groupName as AlterCopyTable,
'ALTER TABLE '+schema_name(o.schema_id)+'.'+object_name(i.object_id)+'_temp DROP CONSTRAINT CONS_'+object_name (i.object_id)+'_temp' as DropConstraint,
'CREATE CLUSTERED COLUMNSTORE INDEX CCI_'+object_name(i.object_id)+'_temp ON '+schema_name(o.schema_id)+'.'+object_name (i.object_id)+'_temp WITH (DROP_EXISTING = OFF)'  as CreateIndex,
'ALTER TABLE '+schema_name (o.schema_id)+'.'+object_name (i.object_id)
+' SWITCH PARTITION '+CAST (MAX(p.partition_number) AS VARCHAR(10))+' TO '+schema_name(o.schema_id)+'.'+object_name (i.object_id)+'_temp' as SwitchPartition,
'ALTER TABLE '+schema_name (o.schema_id)+'.'+object_name(i.object_id)+'_temp ADD CONSTRAINT check_date'+object_name (i.object_id)+'
CHECK ('+Col. PartitioningColumn+'<'''+CONVERT(VARCHAR,@SplitDate,20)+''' AND '+Col. PartitioningColumn+' >= '''+CONVERT(VARCHAR,DATEADD(month,@PartitionSizeMonths*(-1),@SplitDate),20) +''' AND '+Col. PartitioningColumn+' IS NOT NULL)'
as CreateDateConstraint,
'ALTER PARTITION SCHEME ['+Col.PartitionScheme+'] NEXT USED ['+p.file_groupName+ ']' as AlterPartition,
'ALTER PARTITION FUNCTION ['+p.pf_name+ '] () SPLIT RANGE ('''+CONVERT(VARCHAR,@SplitDate,20)+''')' as AlterFunction,
'ALTER TABLE '+schema_name(o.schema_id)+'.'+object_name(i.object_id)+'_temp SWITCH TO '+schema_name
(o.schema_id)+'.'+object_name(i.object_id)+' PARTITION '+CAST (MAX (p.partition_number) AS VARCHAR(10)) as SwitchBack, 'SELECT COUNT(*) FROM '+schema_name(o.schema_id) + '.'+object_name (i.object_id)+'_temp ;-- DROP TABLE '+schema_name (o.schema_id)+'. '+object_name(i.object_id)+'_temp' as DropTempTable
INTO #CSI_Switch
FROM sys.indexes AS i
INNER JOIN sys.objects o on i.object_id = o.object_id
INNER JOIN (SELECT 
	t.name AS [TableName], c.name AS [PartitioningColumn], 
	TYPE_NAME(c.user_type_id) AS [Column Type], 
	ps.name AS [PartitionScheme] 
	FROM sys.tables AS t
	JOIN sys.indexes AS i ON t.[object_id] = i.[object_id] AND i.[type] in (5,6) 
	JOIN sys.partition_schemes AS ps ON ps.data_space_id = i.data_space_id 
	JOIN sys.index_columns AS ic ON ic.object_id = i.[object_id] AND ic.index_id = i.index_id AND ic.partition_ordinal >= 1 
	JOIN sys.columns AS c ON t.[object_id] = c.[object_id] AND ic.column_id = c.column_id ) Col ON object_name(i.object_id) = Col.TableName 
INNER JOIN (SELECT 
	DISTINCT o.name as table_name, 
	rv.value as partition_range, 
	f.name as pf_name ,
	fg.name as file_groupName, 
	p.partition_number, p.rows as number_of_rows 
	FROM sys.partitions p 
	INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id 
	INNER JOIN sys.objects o ON p.object_id = o.object_id 
	INNER JOIN sys.system_internals_allocation_units au ON p.partition_id = au.container_id 
	INNER JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id 
	INNER JOIN sys.partition_functions f ON f.function_id = ps.function_id 
	INNER JOIN sys.destination_data_spaces dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number 
	INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id 
	LEFT OUTER JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id) p ON p.table_name = Col.TableName AND p.partition_range IS NULL 
	WHERE i.type in (5,6) 
	GROUP BY i.object_id,i.[name],p.file_groupName,p.pf_name,Col.PartitioningColumn,Col.PartitionScheme,o.schema_id ORDER BY i.[name] 
	GO 

SELECT DB_NAME() AS database_name, sc.name + N'.' + t.name AS table_name, 
CASE si.index_id WHEN 0 THEN N'/* No create statement (Heap) */' 
ELSE 
	CASE is_primary_key 
		WHEN 1 THEN N'ALTER TABLE ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(t.name+'_temp') + N' ADD CONSTRAINT ' + QUOTENAME (si.name+'_temp') + N' PRIMARY KEY ' + 
	CASE 
		WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' 
		ELSE N'CREATE ' + CASE WHEN si.is_unique = 1 THEN N'UNIQUE ' 
		ELSE N'' END + 
	CASE 
		WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' + N'INDEX ' + QUOTENAME(si.name+'_temp') + N' ON ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(t.name+'_temp') + N' ' END 
	+ /* key def */ N'(' + key_definition + N')' 
	+ /* includes */ 
	CASE 
		WHEN include_definition IS NOT NULL THEN N' INCLUDE (' + include_definition + N')' 
		ELSE N'' END + /* filters */ 
	CASE WHEN filter_definition IS NOT NULL THEN N' WHERE ' + filter_definition ELSE N'' END + 
	/* with clause - compression goes here */ 
	CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL 
		THEN N' WITH (' +  
			CASE WHEN row_compression_partition_list IS NOT NULL 
				THEN N'DATA_COMPRESSION = ROW ' ELSE N'' END + 
			CASE WHEN row_compression_partition_list IS NOT NULL AND page_compression_partition_list IS NOT NULL 
				THEN N', ' ELSE N'' END + 
			CASE WHEN page_compression_partition_list IS NOT NULL 
				THEN N'DATA COMPRESSION = PAGE ' ELSE N'' END 
			+ N')' 
		ELSE N'' END 
	END AS index_create_statement, 
	partition_sums.partition_count, 
	si.is_unique, 
	ISNULL(pf.name, '/* Not partitioned */') AS partition_function, 
	ISNULL(psc.name, fg.name) AS partition_scheme_or_filegroup, 
	t.create_date AS table_created_date, 
	t.modify_date AS table_modify_date 
	INTO #Indexes FROM sys.indexes AS si 
		JOIN sys.tables AS t ON si.object_id=t.object_id 
		JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id 
		JOIN #CSI_Switch csw ON t.name = csw.TableName AND sc.name = csw.SchemaName 
		LEFT JOIN sys.dm_db_index_usage_stats AS stat ON stat.database_id = DB_ID() and si.object_id=stat.object_id and si.index_id=stat.index_id 
		LEFT JOIN sys.partition_schemes AS psc ON si.data_space_id=psc.data_space_id 
		LEFT JOIN sys.partition_functions AS pf ON psc.function_id=pf.function_id 
		LEFT JOIN sys.filegroups AS fg ON si.data_space_id=fg.data_space_id 
	/* Key list */ OUTER APPLY ( SELECT STUFF 
		( (SELECT N', ' + QUOTENAME(c.name) + 
			CASE ic.is_descending_key 
				WHEN 1 then N' DESC' ELSE '' END 
			FROM sys.index_columns AS ic 
			JOIN sys.columns AS c ON ic.column_id=c.column_id and ic.object_id=c.object_id 
			WHERE ic.object_id = si.object_id and ic.index_id=si.index_id and ic.key_ordinal > 0 
			ORDER BY ic.key_ordinal FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS keys ( key_definition ) 
	/* Partitioning Ordinal */ OUTER APPLY ( 
		SELECT MAX(QUOTENAME(c.name)) AS column_name 
		FROM sys.index_columns AS ic 
		JOIN sys.columns AS c ON ic.column_id=c.column_id and ic.object_id=c.object_id 
		WHERE ic.object_id = si.object_id and ic.index_id=si.index_id and ic.partition_ordinal = 1) AS partitioning_column 
	/* Include list */ OUTER APPLY ( 
		SELECT STUFF ( (SELECT N', '+ QUOTENAME(c.name) 
		FROM sys.index_columns AS ic 
		JOIN sys.columns AS c ON ic.column_id=c.column_id and ic.object_id=c.object_id 
		WHERE ic.object_id = si.object_id and ic.index_id=si.index_id and ic.is_included_column = 1 
		ORDER BY c.name FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS includes ( include_definition ) 
	/* Partitions */ 
		OUTER APPLY ( 
		SELECT COUNT(*) AS partition_count, 
		CAST(SUM(ps.in_row_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_in_row_GB, 
		CAST(SUM(ps.lob_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_LOB_GB, 
		SUM(ps.row_count) AS row_count FROM sys.partitions AS p 
		JOIN sys.dm_db_partition_stats AS ps ON p.partition_id=ps.partition_id 
		WHERE p.object_id = si.object_id and p.index_id=si.index_id ) AS partition_sums 
	/* row compression list by partition */ 
		OUTER APPLY ( 
		SELECT STUFF( (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32)) 
		FROM sys.partitions AS p 
		WHERE p.object_id = si.object_id and p.index_id=si.index_id and p.data_compression = 1 
		ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS row_compression_clause ( row_compression_partition_list ) 
	/* data compression list by partition */ 
		OUTER APPLY ( 
		SELECT STUFF ( (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32)) 
		FROM sys.partitions AS p 
		WHERE p.object_id = si.object_id and p.index_id=si.index_id and p.data_compression = 2 
		ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS page_compression_clause ( page_compression_partition_list ) 
		WHERE si.type IN (0,1,2) /* heap, clustered, nonclustered */ 
		ORDER BY table_name, si.index_id OPTION (RECOMPILE); 

GO 


DECLARE @sql VARCHAR(1000)

DECLARE CR1 CURSOR FOR
SELECT CreateCopyTable FROM #CSI_Switch

OPEN CR1
PRINT '--Create Temp Table'
FETCH NEXT FROM CR1 INTO @sql

WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT @sql
	FETCH NEXT FROM CR1 INTO @sql
END

CLOSE CR1
DEALLOCATE CR1




DECLARE CR1 CURSOR FOR
SELECT AlterCopyTable FROM #CSI_Switch

OPEN CR1
PRINT '--Alter Temp Table'
FETCH NEXT FROM CR1 INTO @sql

WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT @sql
	FETCH NEXT FROM CR1 INTO @sql
END

CLOSE CR1
DEALLOCATE CR1



DECLARE CR1 CURSOR FOR
SELECT DropConstraint FROM #CSI_Switch

OPEN CR1
PRINT '--Drop Constraints on Temp Table'
FETCH NEXT FROM CR1 INTO @sql

WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT @sql
	FETCH NEXT FROM CR1 INTO @sql
END

CLOSE CR1
DEALLOCATE CR1



DECLARE CR1 CURSOR FOR
SELECT CreateIndex FROM #CSI_Switch

OPEN CR1
PRINT '--Create CI on Temp Table'
FETCH NEXT FROM CR1 INTO @sql

WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT @sql
	FETCH NEXT FROM CR1 INTO @sql
END

CLOSE CR1
DEALLOCATE CR1




DECLARE CR1 CURSOR FOR
SELECT index_create_statement FROM #Indexes

OPEN CR1
PRINT '--Create all indexes on Temp Table'
FETCH NEXT FROM CR1 INTO @sql

WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT @sql
	FETCH NEXT FROM CR1 INTO @sql
END

CLOSE CR1
DEALLOCATE CR1


--Add transaction handling 
PRINT 'BEGIN TRY' 
PRINT ' BEGIN TRANSACTION' 
DECLARE CR1 CURSOR FOR SELECT SwitchPartition FROM #CSI_Switch 
OPEN CR1 
PRINT '--Switch Partitions' 
FETCH NEXT FROM CR1 INTO @sql 
WHILE @@FETCH_STATUS = 0 
BEGIN 
	PRINT @sql 
	FETCH NEXT FROM CR1 INTO @sql 
END 
CLOSE CR1 
DEALLOCATE CR1 


DECLARE CR1 CURSOR FOR SELECT CreateDateConstraint FROM #CSI_Switch 
OPEN CR1 
PRINT '--Create date Constraint on Temp' 
FETCH NEXT FROM CR1 INTO @sql 
WHILE @@FETCH_STATUS = 0 
BEGIN 
	PRINT @sql	
	FETCH NEXT FROM CR1 INTO @sql 
END
CLOSE CR1 
DEALLOCATE CR1 

PRINT '--Create additional partition' 
Select TOP 1 @sql = AlterPartition FROM #CSI_Switch 
PRINT @sql 
Select TOP 1 @sql = AlterFunction FROM #CSI_Switch 
PRINT @sql 
DECLARE CR1 CURSOR FOR SELECT SwitchBack FROM #CSI_Switch 
OPEN CR1 
PRINT '--Switch partition back to original table' 
FETCH NEXT FROM CR1 INTO @sql 
WHILE @@FETCH_STATUS = 0 
BEGIN 
	PRINT @sql 
	FETCH NEXT FROM CR1 INTO @sql 
END 
CLOSE CR1 
DEALLOCATE CR1 

PRINT ' COMMIT TRAN' 
PRINT 'END TRY' 
PRINT 'BEGIN CATCH' 
PRINT ' IF (@@TRANCOUNT > 0) ' 
PRINT ' BEGIN' 
PRINT '		ROLLBACK TRAN' 
PRINT '		PRINT ''Error detected, all changes reversed''' 
PRINT ' END ' 
PRINT ' SELECT' 
PRINT '		ERROR_NUMBER() AS ErrorNumber,' 
PRINT '		ERROR_SEVERITY() AS ErrorSeverity,' 
PRINT '		ERROR_STATE() AS ErrorState,' 
PRINT '		ERROR_PROCEDURE() AS ErrorProcedure,' 
PRINT '		ERROR_LINE() AS ErrorLine,' 
PRINT '		ERROR_MESSAGE() AS ErrorMessage' 
PRINT 'END CATCH' 

DECLARE CR1 CURSOR 
FOR SELECT DropTempTable FROM #CSI_Switch 
OPEN CR1 
PRINT '--Drop Temp Tables' 
FETCH NEXT FROM CR1 INTO @sql 
WHILE @@FETCH_STATUS = 0 
BEGIN 
	PRINT @sql	
	FETCH NEXT FROM CR1 INTO @sql 
END 
CLOSE CR1 
DEALLOCATE CR1 


SELECT * FROM #Indexes