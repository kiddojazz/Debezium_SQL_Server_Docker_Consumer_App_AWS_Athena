-- 1. Create schema
create schema logis;

-- 2. View and Count Table

select top 20 * from [logis].[tickets]


select count(1) from [logis].[tickets]

-- 3. GRANT USER ADMIN Priviledge
SELECT CURRENT_USER

ALTER SERVER ROLE [sysadmin] ADD MEMBER temidayo;

-- Check login exists
SELECT name, type_desc FROM sys.server_principals WHERE name = 'temidayo';

-- Check login is in sysadmin role
SELECT l.name
FROM sys.server_role_members rm
JOIN sys.server_principals r ON rm.role_principal_id = r.principal_id
JOIN sys.server_principals l ON rm.member_principal_id = l.principal_id
WHERE r.name = 'sysadmin';


-- 4. Enable CDC at the Database Level
USE TicketingDB;
EXEC sys.sp_cdc_enable_db;


-- Enable CDC on specific tables
EXEC sys.sp_cdc_enable_table
    @source_schema = N'logis',
    @source_name = N'tickets',
    @role_name = NULL,
    @supports_net_changes = 0;

-- Check If CDC Is Enabled

-- Check if it was enabled
SELECT is_cdc_enabled FROM sys.databases WHERE name = 'TicketingDB';

-- At table level
SELECT * FROM cdc.change_tables;



