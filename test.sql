PRINT('======== BEGIN SETUP ========');
PRINT('');
PRINT('SETUP: Switching to [destination_db]');
USE [destination_db];
-- BEGIN CREATE FUNCTIONS --

PRINT('SETUP: Creating look-up functions');
PRINT('');
GO

CREATE FUNCTION dbo.get_individual_individual_id_by_orig_agent_id(@value AS INT)
RETURNS UNIQUEIDENTIFIER
BEGIN
    DECLARE @ret UNIQUEIDENTIFIER;
     SELECT @ret = individual_id
       FROM [individual]
      WHERE [orig_agent_id] = @value;
     RETURN @ret;
END;
GO


-- END CREATE FUNCTIONS --

PRINT('SETUP: Executing Setup Commands');
PRINT('');

PRINT('========= END SETUP =========');
PRINT('');
PRINT('===================================================');
PRINT('=!!!!! TEST MODE: TRANSACTION IS ROLLED BACK !!!!!=');
PRINT('===================================================');
PRINT('');
BEGIN TRANSACTION;

-- BEGIN CLEAN UP SQL --

PRINT('======== BEGIN CLEANUP ========');
PRINT('');
-- PRINT('INFO: Deleting all records in [destination_db].[dbo].[user_roles] where orig_user_id IS NOT NULL');
-- DELETE FROM [destination_db].[dbo].[user_roles] WHERE orig_user_id IS NOT NULL;
-- PRINT('');
-- PRINT('INFO: Deleting all records in [destination_db].[dbo].[users] where orig_id IS NOT NULL');
-- DELETE FROM [destination_db].[dbo].[users] WHERE orig_id IS NOT NULL;
-- PRINT('');
PRINT('========= END CLEANUP =========');
PRINT('');

-- END CLEAN UP SQL --


-- BEGIN INSERT-SELECT SQL --

PRINT('======== BEGIN PASS 1 of 2 ========');
PRINT('');
PRINT('==== Merging [destination_db].[dbo].[users] with [source_db].[dbo].[etl_Users] =====');
PRINT('INFO: migrating NON-DUPLICATE users');
MERGE [destination_db].[dbo].[users] AS D
USING [source_db].[dbo].[etl_Users] AS S
   ON D.[orig_id] = S.[UserID]
WHEN MATCHED THEN
    UPDATE SET
    [orig_id] = S.[UserID],
    [orig_admin] = S.[Admin],
    [user_id] = S.[Username],
    [password] = NULL,
    [user_nm] = RTRIM(LTRIM(COALESCE(S.[FirstName], '') + ' ' + COALESCE(S.[LastName], ''))),
    [attr_bits] = CASE WHEN S.[IsActive] = 1 THEN 0 ELSE 1 END,
    [email_address] = S.[EMailAddress],
    [user_guid] = dbo.get_individual_individual_id_by_orig_agent_id(S.[AgentID]),
    [first_name] = S.[FirstName],
    [last_name] = S.[LastName],
    [date_created] = S.[RegistrationDate],
    [pin_code] = S.[PinCode]
WHEN NOT MATCHED THEN
    INSERT (
        [orig_id],
        [orig_admin],
        [user_id],
        [password],
        [user_nm],
        [attr_bits],
        [email_address],
        [user_guid],
        [first_name],
        [last_name],
        [date_created],
        [pin_code]
    ) VALUES (
        S.[UserID],
        S.[Admin],
        S.[Username],
        NULL,
        RTRIM(LTRIM(COALESCE(S.[FirstName], '') + ' ' + COALESCE(S.[LastName], ''))),
        CASE WHEN S.[IsActive] = 1 THEN 0 ELSE 1 END,
        S.[EMailAddress],
        dbo.get_individual_individual_id_by_orig_agent_id(S.[AgentID]),
        S.[FirstName],
        S.[LastName],
        S.[RegistrationDate],
        S.[PinCode]
    )
OUTPUT $action,
       Inserted.[orig_id],
       Inserted.[orig_admin],
       Inserted.[user_id],
       Inserted.[password],
       Inserted.[user_nm],
       Inserted.[attr_bits],
       Inserted.[email_address],
       Inserted.[user_guid],
       Inserted.[first_name],
       Inserted.[last_name],
       Inserted.[date_created],
       Inserted.[pin_code];
PRINT('');

PRINT('==== Merging [destination_db].[dbo].[user_roles] with [source_db].[dbo].[etl_Users] =====');
PRINT('INFO: defaulted role for imported to Users');
MERGE [destination_db].[dbo].[user_roles] AS D
USING [source_db].[dbo].[etl_Users] AS S
   ON D.[orig_user_id] = S.[UserID]
WHEN MATCHED THEN
    UPDATE SET
    [orig_user_id] = S.[UserID],
    [user_id] = S.[Username],
    [role_nm] = 'Users'
WHEN NOT MATCHED THEN
    INSERT (
        [orig_user_id],
        [user_id],
        [role_nm]
    ) VALUES (
        S.[UserID],
        S.[Username],
        'Users'
    )
OUTPUT $action,
       Inserted.[orig_user_id],
       Inserted.[user_id],
       Inserted.[role_nm];
PRINT('');

PRINT('======= END OF PASS 1 of 2 ========');
PRINT('');
PRINT('======== BEGIN PASS 2 of 2 ========');
PRINT('');
PRINT('==== Merging [destination_db].[dbo].[users] with [source_db].[dbo].[etl_Users] =====');
PRINT('INFO: migrating NON-DUPLICATE users; and non-netquarry users by');
MERGE [destination_db].[dbo].[users] AS D
USING [source_db].[dbo].[etl_Users] AS S
   ON D.[orig_id] = S.[UserID]
WHEN MATCHED THEN
    UPDATE SET
    [orig_id] = S.[UserID],
    [orig_admin] = S.[Admin],
    [user_id] = S.[Username],
    [password] = NULL,
    [user_nm] = RTRIM(LTRIM(COALESCE(S.[FirstName], '') + ' ' + COALESCE(S.[LastName], ''))),
    [attr_bits] = CASE WHEN S.[IsActive] = 1 THEN 0 ELSE 1 END,
    [email_address] = S.[EMailAddress],
    [user_guid] = dbo.get_individual_individual_id_by_orig_agent_id(S.[AgentID]),
    [first_name] = S.[FirstName],
    [last_name] = S.[LastName],
    [date_created] = S.[RegistrationDate],
    [pin_code] = S.[PinCode]
WHEN NOT MATCHED THEN
    INSERT (
        [orig_id],
        [orig_admin],
        [user_id],
        [password],
        [user_nm],
        [attr_bits],
        [email_address],
        [user_guid],
        [first_name],
        [last_name],
        [date_created],
        [pin_code]
    ) VALUES (
        S.[UserID],
        S.[Admin],
        S.[Username],
        NULL,
        RTRIM(LTRIM(COALESCE(S.[FirstName], '') + ' ' + COALESCE(S.[LastName], ''))),
        CASE WHEN S.[IsActive] = 1 THEN 0 ELSE 1 END,
        S.[EMailAddress],
        dbo.get_individual_individual_id_by_orig_agent_id(S.[AgentID]),
        S.[FirstName],
        S.[LastName],
        S.[RegistrationDate],
        S.[PinCode]
    )
OUTPUT $action,
       Inserted.[orig_id],
       Inserted.[orig_admin],
       Inserted.[user_id],
       Inserted.[password],
       Inserted.[user_nm],
       Inserted.[attr_bits],
       Inserted.[email_address],
       Inserted.[user_guid],
       Inserted.[first_name],
       Inserted.[last_name],
       Inserted.[date_created],
       Inserted.[pin_code];
PRINT('');

PRINT('==== Merging [destination_db].[dbo].[user_roles] with [source_db].[dbo].[etl_Users] =====');
PRINT('INFO: defaulted role for imported to Users');
MERGE [destination_db].[dbo].[user_roles] AS D
USING [source_db].[dbo].[etl_Users] AS S
   ON D.[orig_user_id] = S.[UserID]
WHEN MATCHED THEN
    UPDATE SET
    [orig_user_id] = S.[UserID],
    [user_id] = S.[Username],
    [role_nm] = 'Users'
WHEN NOT MATCHED THEN
    INSERT (
        [orig_user_id],
        [user_id],
        [role_nm]
    ) VALUES (
        S.[UserID],
        S.[Username],
        'Users'
    )
OUTPUT $action,
       Inserted.[orig_user_id],
       Inserted.[user_id],
       Inserted.[role_nm];
PRINT('');

PRINT('======= END OF PASS 2 of 2 ========');
PRINT('');
-- END INSERT-SELECT SQL -- 

PRINT('===================================================');
PRINT('=!!!!!! TEST MODE: ROLLING BACK TRANSACTION !!!!!!=');
PRINT('===================================================');
rollback;
PRINT('');
PRINT('======== BEGIN TEARDOWN ========');
PRINT('');
GO

-- BEGIN DROP FUNCTIONS --

PRINT('TEARDOWN: Dropping Look-up Functions');
PRINT('');
GO

DROP FUNCTION dbo.get_individual_individual_id_by_orig_agent_id;

-- END DROP FUNCTIONS --

PRINT('TEARDOWN: Executing Teardown Commands');
PRINT('');
PRINT('========= END TEARDOWN =========');
