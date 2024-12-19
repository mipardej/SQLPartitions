USE [master]
GO

CREATE DATABASE [PartitionDemo2]
 ON  PRIMARY 
( NAME = N'PartitionDemo2', FILENAME = N'/var/opt/mssql/data/PartitionDemo2.mdf' ), 
 FILEGROUP [FG2022] 
( NAME = N'PartitionDemo2_2022', FILENAME = N'/var/opt/mssql/data/PartitionDemo2_2022.ndf' ), 
 FILEGROUP [FG2023] 
( NAME = N'PartitionDemo2_2023', FILENAME = N'/var/opt/mssql/data/PartitionDemo2_2023.ndf' ), 
 FILEGROUP [FG2024] 
( NAME = N'PartitionDemo2_2024', FILENAME = N'/var/opt/mssql/data/PartitionDemo2_2024.ndf' )
 LOG ON 
( NAME = N'PartitionDemo2_log', FILENAME = N'/var/opt/mssql/data/PartitionDemo2_log.ldf' )

GO

