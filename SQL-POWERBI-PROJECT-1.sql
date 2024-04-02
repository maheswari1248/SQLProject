--SELECT * FROM DBO.LOANDATA
--SP_HELP 'DBO.LOANDATA'

--CREATING STG TABLE WITH HASHBYTES

CREATE SCHEMA STG


create table STG.STAGING(

										OLd_loan_no INT,
										Member_name varchar(60),
										Date_of_birth DATE,
										Age TINYINT,
										disbursement_date DATE,
										PURPOSE_ID varchar(60),
										state varchar(30),
										status varchar(30),
										loan_created_on DATE,
										branch_name varchar(60),
										PRINCIPAL_TOTAL INT,
										prin_collected INT,
										INT_COLLECCTED INT,
										last_maturity_date DATE,
										int_rate FLOAT,
										processing_fee INT,
										tenure FLOAT,
										total_interest INT,
										total_instal INT,
										LOAN_CLOSURE_DATE DATE,
										Insurance_Charges INT,
										insurance_maturity_date DATE,
										outstanding_principal INT,
										OUTSTANDING_INTEREST INT,
										customer_id INT,
										RowHash as convert(varbinary(64),HASHBYTES('SHA2_256', CONCAT( OLd_loan_no,Member_name,Date_of_birth,Age,disbursement_date,PURPOSE_ID,state,status,loan_created_on,branch_name,PRINCIPAL_TOTAL,prin_collected,
										INT_COLLECCTED,last_maturity_date,int_rate,processing_fee,tenure,total_interest,total_instal,LOAN_CLOSURE_DATE,insurance_maturity_date,outstanding_principal,
										OUTSTANDING_INTEREST,customer_id)))
)

--selecting the new records from existing staging table using left join new staging table
--select * from dbo.[[dbo]].[duplicate]]]

TRUNCATE TABLE STG.STAGING
TRUNCATE TABLE DIM.CUSTOMER
TRUNCATE TABLE DIM.LOAN

create procedure uspRowhashInsert
as
begin
insert into STG.STAGING(

										OLd_loan_no,
										Member_name,
										Date_of_birth ,
										Age ,
										disbursement_date,
										PURPOSE_ID,
										state ,
										status ,
										loan_created_on ,
										branch_name ,
										PRINCIPAL_TOTAL ,
										prin_collected ,
										INT_COLLECCTED ,
										last_maturity_date ,
										int_rate ,
										processing_fee ,
										tenure ,
										total_interest,
										total_instal ,
										LOAN_CLOSURE_DATE ,
										Insurance_Charges,
										insurance_maturity_date ,
										outstanding_principal ,
										OUTSTANDING_INTEREST ,
										customer_id
)
select 
		a.OLd_loan_no,
		a.Member_name,
		a.Date_of_birth ,
		a.Age ,
		a.disbursement_date,
		a.PURPOSE_ID,
		a.state ,
		a.status ,
		a.loan_created_on ,
		a.branch_name ,
		a.PRINCIPAL_TOTAL ,
		a.prin_collected ,
		a.INT_COLLECCTED ,
		a.last_maturity_date ,
		a.int_rate ,
		a.processing_fee ,
		a.tenure ,
		a.total_interest,
		a.total_instal ,
		a.LOAN_CLOSURE_DATE ,
		a.Insurance_Charges,
		a.insurance_maturity_date ,
		a.outstanding_principal ,
		a.OUTSTANDING_INTEREST ,
		a.customer_id  from dbo.loandata a
left join STG.LaonSTAGING b
on a.OLD_LOAN_NO=b.OLd_loan_no
where b.OLd_loan_no is null
end

--EXEC uspRowhashInsert
--SELECT COUNT(*) FROM STG.STAGING

--DIMCUSTOMER TABLE ---

--CREATE SCHEMA DIM

CREATE TABLE DIM.CUSTOMER(
PK_CUSTOMERID INT IDENTITY(1,1),
CUSTOMERID INT,
MEMBERNAME VARCHAR(60),
DATEOFBIRTH DATE,
AGE INT,
EFF_STARTDATE DATE DEFAULT GETDATE(),
EFF_ENDDATE DATE DEFAULT NULL
)

truncate table dim.customer
Alter procedure uspCustomers
as
begin
    DECLARE @objname VARCHAR(20), @startdate DATETIME, @endate DATETIME, @no_of_rowsinserted INT, @no_of_rowsupdated INT ,@errormessage varchar(30)
truncate table dim.customer
MERGE INTO DIM.CUSTOMER AS TARGET
USING (SELECT
				DISTINCT CUSTOMER_ID ,
				MAX(MEMBER_NAME) AS MEMBERNAME ,
				MAX(DATE_OF_BIRTH) AS DATEOFBIRTH,
				MAX(AGE) AS AGE
		FROM STG.LoanSTAGING
		GROUP BY CUSTOMER_ID
	   ) AS SOURCE
ON TARGET.CUSTOMERID=SOURCE.CUSTOMER_ID 
WHEN MATCHED AND (SOURCE.MEMBERNAME<>TARGET.MEMBERNAME OR SOURCE.DATEOFBIRTH<>TARGET.DATEOFBIRTH OR SOURCE.AGE<>TARGET.AGE)
--updating existing records--
THEN UPDATE SET 
TARGET.EFF_ENDDATE=GETDATE()---@no_of_rowsupdated = @@rowcount
WHEN NOT MATCHED BY TARGET THEN
INSERT (CUSTOMERID,MEMBERNAME,DATEOFBIRTH,AGE)
VALUES(SOURCE.CUSTOMER_ID,SOURCE.MEMBERNAME,SOURCE.DATEOFBIRTH,SOURCE.AGE);

--set @no_of_rowsinserted = @@rowcount

--inserting new records--
INSERT INTO DIM.CUSTOMER(CUSTOMERID,MEMBERNAME,DATEOFBIRTH,AGE)
SELECT SOURCE.CUSTOMER_ID,SOURCE.MEMBER_NAME,SOURCE.DATE_OF_BIRTH,SOURCE.AGE
FROM stg.LoanSTAGING source
left join DIM.CUSTOMER b
on source.Customer_ID=b.customerid
where b.eff_enddate is not null
INSERT INTO auditing VALUES (@objname, @startdate, @endate, @no_of_rowsinserted, @no_of_rowsupdated, @errormessage);

end
select * from auditing
select * from dim.CUSTOMER
select * from dim.CUSTOMER where EFF_ENDDATE is not null
--truncate table dim.customer
exec uspCustomers
-----------------WITH BEGIN TRANSACTION,TRY,CATCH--------------------------------




CREATE PROCEDURE uspCustomers
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE INTO DIM.CUSTOMER AS TARGET
        USING (SELECT
                    DISTINCT CUSTOMER_ID ,
                    MAX(MEMBER_NAME) AS MEMBERNAME ,
                    MAX(DATE_OF_BIRTH) AS DATEOFBIRTH,
                    MAX(AGE) AS AGE
            FROM STG.LoanSTAGING
            GROUP BY CUSTOMER_ID
           ) AS SOURCE
        ON TARGET.CUSTOMERID=SOURCE.CUSTOMER_ID 
        WHEN MATCHED AND (SOURCE.MEMBERNAME<>TARGET.MEMBERNAME OR SOURCE.DATEOFBIRTH<>TARGET.DATEOFBIRTH OR SOURCE.AGE<>TARGET.AGE)
        --updating existing records--
        THEN UPDATE SET 
        TARGET.EFF_ENDDATE=GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
        INSERT (CUSTOMERID,MEMBERNAME,DATEOFBIRTH,AGE)
        VALUES(SOURCE.CUSTOMER_ID,SOURCE.MEMBERNAME,SOURCE.DATEOFBIRTH,SOURCE.AGE);


        --inserting new records--

        INSERT INTO DIM.CUSTOMER(CUSTOMERID,MEMBERNAME,DATEOFBIRTH,AGE)
        SELECT SOURCE.CUSTOMER_ID,SOURCE.MEMBER_NAME,SOURCE.DATE_OF_BIRTH,SOURCE.AGE
        FROM STG.LoanStaging source
        LEFT JOIN DIM.CUSTOMER b
        ON source.Customer_ID=b.customerid
        WHERE b.eff_enddate IS NOT NULL;
		update stg.LoanStaging set Member_name='bhargavi' where customer_id=49086
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise error or log the error message
        DECLARE @ErrorMessage NVARCHAR(MAX);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;


------------------------------------------------------------------------------------------



--SELECT COUNT(*) FROM DIM.CUSTOMER
--EXEC uspCustomers


--DIMLOAN--

CREATE TABLE DIM.LOAN(
PK_LOANID INT IDENTITY(1,1),
OLDLOANNO INT,
PURPOSEID VARCHAR(60),
DISBURSEMENTDATE DATE,
LOANCREATEDON DATE,
LASTMATURITYDATE DATE,
STATUS VARCHAR(20),
LOANCLOSUREDATE DATE,
INSURANCEMATURITYDATE DATE,
EFF_STARTDATE DATE DEFAULT GETDATE(),
EFF_ENDDATE DATE DEFAULT NULL
)



create or alter procedure uspLoans
as
begin
MERGE DIM.LOAN AS target
USING (
       SELECT DISTINCT
						Old_Loan_No,
						PURPOSE_ID,
						DISBURSEMENT_DATE,--convert(datetime,DISBURSEMENT_DATE,101)as DISBURSEMENT_DATE,
						LOAN_CREATED_ON,--convert(datetime,LOAN_CREATED_ON,101)as LOAN_CREATED_ON,
						LAST_MATURITY_DATE,--convert(datetime,LAST_MATURITY_DATE,101)as LAST_MATURITY_DATE,
						STATUS,
						LOAN_CLOSURE_DATE,--convert(datetime,LOAN_CLOSURE_DATE,101)as LOAN_CLOSURE_DATE,
						INSURANCE_MATURITY_DATE--convert(datetime,INSURANCE_MATURITY_DATE,101)as INSURANCE_MATURITY_DATE
    FROM STG.LoanSTAGING
) AS source
ON target.OldLoanNo = source.Old_Loan_NO-- and target.status<>source.status or 
--target.LastMaturityDate<>source.Last_Maturity_Date or target.LoanClosureDate<>source.Loan_Closure_Date or 
--target.InsuranceMaturityDate<>source.Insurance_Maturity_Date 

WHEN MATCHED  and (target.status<>source.status or 
target.LastMaturityDate<>source.Last_Maturity_Date or target.LoanClosureDate<>source.Loan_Closure_Date or 
target.InsuranceMaturityDate<>source.Insurance_Maturity_Date or target.purposeid<>source.purpose_id )
THEN
    UPDATE SET
		target.Eff_EndDate=getdate()
		

when NOT MATCHED BY TARGET THEN
    INSERT (
				OldLoanNo,
				PurposeId,
				DisbursementDate,
				LoanCreatedOn,
				LastMaturityDate,
				Status,
				LoanClosureDate,
				InsuranceMaturityDate
		   ) 
	VALUES (
				source.Old_Loan_NO,
				source.PURPOSE_ID,
				source.DISBURSEMENT_DATE,
				source.LOAN_CREATED_ON,
				source.LAST_MATURITY_DATE,
				source.STATUS,
				source.LOAN_CLOSURE_DATE,
				source.INSURANCE_MATURITY_DATE
    );

	--inserting the updated records into loandimension table using left join

	insert into Dim.Loan(  
							OldLoanNo,
							PurposeId,
							DisbursementDate,
							LoanCreatedOn,
							LastMaturityDate,
							Status,
							LoanClosureDate,
							InsuranceMaturityDate
						 )
				select 
							source.Old_Loan_NO,
							source.PURPOSE_ID,
							source.DISBURSEMENT_DATE,
							source.LOAN_CREATED_ON,
							source.LAST_MATURITY_DATE,
							source.STATUS,
							source.LOAN_CLOSURE_DATE,
							source.INSURANCE_MATURITY_DATE
	from STG.LoanStaging source
	left join dim.Loan target
	on target.OldLoanNo=source.old_loan_no
	where target.EFF_ENDDATE is not null
end

SELECT * FROM DIM.LOAN where OLdloanno=761867
EXEC uspLoans
update stg.LoanStaging set PURPOSE_ID='xyentas'
where OLd_loan_no=761867
----------------------------LOAN TABLE WITH BEGIN TRANSACTION TRY,CATCH-------------------------------


CREATE OR ALTER PROCEDURE uspLoans
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE INTO DIM.LOAN AS target
        USING (
               SELECT DISTINCT
                                Old_Loan_No,
                                PURPOSE_ID,
                                DISBURSEMENT_DATE,
                                LOAN_CREATED_ON,
                                LAST_MATURITY_DATE,
                                STATUS,
                                LOAN_CLOSURE_DATE,
                                INSURANCE_MATURITY_DATE
                FROM STG.LoanSTAGING
            ) AS source
        ON target.OldLoanNo = source.Old_Loan_NO

        WHEN MATCHED AND (target.Status <> source.STATUS OR 
                          target.LastMaturityDate <> source.Last_Maturity_Date OR 
                          target.LoanClosureDate <> source.Loan_Closure_Date OR 
                          target.InsuranceMaturityDate <> source.Insurance_Maturity_Date)
        THEN
            UPDATE SET target.Eff_EndDate = GETDATE();

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                        OldLoanNo,
                        PurposeId,
                        DisbursementDate,
                        LoanCreatedOn,
                        LastMaturityDate,
                        Status,
                        LoanClosureDate,
                        InsuranceMaturityDate
                   ) 
            VALUES (
                        source.Old_Loan_NO,
                        source.PURPOSE_ID,
                        source.DISBURSEMENT_DATE,
                        source.LOAN_CREATED_ON,
                        source.LAST_MATURITY_DATE,
                        source.STATUS,
                        source.LOAN_CLOSURE_DATE,
                        source.INSURANCE_MATURITY_DATE
            );

        -- Inserting the updated records into loandimension table using left join

        INSERT INTO Dim.Loan (
                                OldLoanNo,
                                PurposeId,
                                DisbursementDate,
                                LoanCreatedOn,
                                LastMaturityDate,
                                Status,
                                LoanClosureDate,
                                InsuranceMaturityDate
                             )
        SELECT 
                    source.Old_Loan_NO,
                    source.PURPOSE_ID,
                    source.DISBURSEMENT_DATE,
                    source.LOAN_CREATED_ON,
                    source.LAST_MATURITY_DATE,
                    source.STATUS,
                    source.LOAN_CLOSURE_DATE,
                    source.INSURANCE_MATURITY_DATE
        FROM STG.LoanSTAGING source
        LEFT JOIN dim.Loan target ON target.OldLoanNo = source.old_loan_no
        WHERE target.OldLoanNo IS NULL;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise error or log the error message
        DECLARE @ErrorMessage NVARCHAR(MAX);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;

------------------------------------------------------------------------------------------------

---DIMSTATE--
truncate table dim.state
drop table dim.state
CREATE TABLE DIM.STATE(
PK_STATEID INT IDENTITY(1,1),
STATE VARCHAR(30),
EFF_STARTDATE DATE DEFAULT GETDATE(),
EFF_ENDDATE DATE DEFAULT NULL
)

alter PROCEDURE USPSTATE
AS
BEGIN
MERGE INTO DIM.STATE AS TARGET
USING (
		SELECT DISTINCT STATE
		FROM STG.LoanSTAGING where state is not null
		)
 AS SOURCE
ON SOURCE.STATE=TARGET.STATE
WHEN MATCHED AND SOURCE.STATE<>TARGET.STATE 
THEN
UPDATE SET target.eff_enddate=getdate()
WHEN NOT MATCHED BY TARGET THEN
INSERT(STATE)
VALUES(SOURCE.STATE);
insert into dim.state(state)
select  s.state
FROM STG.LoanStaging  STG
		LEFT OUTER JOIN DIM.STATE S
		ON STG.state=S.STATE
		where s.state is null and s.EFF_ENDDATE is  not null






select * from dim.STATE where EFF_ENDDATE is not null
END
truncate table dim.state
--SELECT * FROM DIM.STATE
EXEC USPSTATE

update stg.LoanStaging
set 
state='up '
where state='uttarpradesh'
select * from dim.state

--------------------------------DIM.STATE_BEGIN TRANSACTION,TRY,CATCH---------------------------------------

CREATE PROCEDURE USPSTATE
AS
BEGIN
    BEGIN TRY
	  
        BEGIN TRANSACTION;

        MERGE INTO DIM.STATE AS TARGET
        USING (
                SELECT DISTINCT STATE
                FROM STG.LoanSTAGING
              ) AS SOURCE
        ON SOURCE.STATE = TARGET.STATE
        WHEN MATCHED AND SOURCE.STATE <> TARGET.STATE 
        THEN
            UPDATE SET TARGET.STATE = SOURCE.STATE
        WHEN NOT MATCHED BY TARGET 
        THEN
            INSERT (STATE)
            VALUES (SOURCE.STATE);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise error or log the error message
        DECLARE @ErrorMessage NVARCHAR(MAX);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;

------------------------------------------------------------------------------------------------------------------------------

--DIMBRANCH--
drop table dim.BRANCH
CREATE TABLE DIM.BRANCH(
PK_BRANCHID INT IDENTITY(1,1),
BRANCHNAME VARCHAR(60),
EFF_STARTDATE DATE DEFAULT GETDATE(),
EFF_ENDDATE DATE DEFAULT NULL
)

CREATE PROCEDURE USPBRANCH
AS
BEGIN
MERGE INTO DIM.BRANCH AS TARGET
USING (
		SELECT DISTINCT BRANCH_NAME
		FROM STG.LoanSTAGING
	   )AS SOURCE
ON SOURCE.BRANCH_NAME=TARGET.BRANCHNAME
WHEN MATCHED AND SOURCE.BRANCH_NAME<>TARGET.BRANCHNAME
THEN
UPDATE SET TARGET.BRANCHNAME=SOURCE.BRANCH_NAME
WHEN NOT MATCHED BY TARGET 
THEN
INSERT(BRANCHNAME)
VALUES(SOURCE.BRANCH_NAME);
insert into dim.BRANCH(BRANCHNAME)
select stg.branch_name from stg.STAGING stg
left outer join dim.BRANCH b
on stg.branch_name=b.BRANCHNAME
where b.BRANCHNAME is null

END

exec USPBRANCH
------------------------DIM.BRANCH_BEGIN TRANSACTION,TRY,CATCH-----------------------------------------------

Alter PROCEDURE USPBRANCH
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE INTO DIM.BRANCH AS TARGET
        USING (
                SELECT DISTINCT BRANCH_NAME
                FROM STG.LoanSTAGING
              ) AS SOURCE
        ON SOURCE.BRANCH_NAME = TARGET.BRANCHNAME
        WHEN MATCHED AND SOURCE.BRANCH_NAME <> TARGET.BRANCHNAME
        THEN
            UPDATE SET TARGET.BRANCHNAME = SOURCE.BRANCH_NAME
        WHEN NOT MATCHED BY TARGET 
        THEN
            INSERT (BRANCHNAME)
            VALUES (SOURCE.BRANCH_NAME);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise error or log the error message
        DECLARE @ErrorMessage NVARCHAR(MAX);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;
exec

-------------------------------------------------------------------------------------------------------------

--SELECT * FROM DIM.CUSTOMER
--SELECT * FROM DIM.LOAN
--SELECT * FROM DIM.BRANCH
--SELECT * FROM DIM.STATE

CREATE SCHEMA FACT
CREATE TABLE FACT.LOANDETAILS(
			Fk_BranchId INT,
            FK_CustomerId INT,
            FK_LoanID INT,
            FK_StateId INT,
            PrincipalTotal INT,
            PrinCollected INT,
            IntCollected INT,
            IntRate FLOAT,
            ProcessingFee INT,
            Tenure FLOAT,
            TOTALINTEREST INT,
            TOTALINSTAL INT,
            INSURANCECHARGES INT,
            OUTSTANDINGPRINCIPAL INT,
            OutstandingInterest INT,
            rowhash VARBINARY(64)
			)


--alter PROCEDURE uspFact
--AS
--BEGIN
--    DECLARE @BatchSize INT = 50000; -- Specify your batch size
--    DECLARE @RowCount INT = 1; -- Initialize the row count

--    WHILE @RowCount > 0
--    BEGIN
--        INSERT INTO Fact.LOANDETAILS(
--            Fk_BranchId,
--            FK_CustomerId,
--            FK_LoanID,
--            FK_StateId,
--            PrincipalTotal,
--            PrinCollected,
--            IntCollected,
--            IntRate,
--            ProcessingFee,
--            Tenure,
--            TOTALINTEREST,
--            TOTALINSTAL,
--            INSURANCECHARGES,
--            OUTSTANDINGPRINCIPAL,
--            OutstandingInterest,
--            rowhash
--        )
--        SELECT TOP (@BatchSize)
--            d.Pk_BranchId,
--            b.pk_Customerid,
--            c.Pk_LoanId,
--            e.Pk_StateId,
--            a.Principal_Total,
--            a.PRIN_COLLECTED,
--            a.INT_COLLECCTED,
--            a.Int_Rate,
--            a.Processing_Fee,
--            a.Tenure,
--            a.TOTAL_INTEREST,
--            a.TOTAL_INSTAL,
--            a.INSURANCE_CHARGES,
--            a.OUTSTANDING_PRINCIPAL,
--            a.Outstanding_Interest,
--            a.Rowhash
--        FROM STG.STAGING A
--        LEFT JOIN Fact.LOANDETAILS F ON A.RowHash = F.Rowhash  
--        INNER JOIN dim.Customer b ON a.Customer_ID = b.CustomerId AND b.Eff_Enddate IS NULL
--        INNER JOIN dim.Loan c ON a.OLD_LOAN_NO = c.OldLoanNo AND c.Eff_Enddate IS NULL
--        INNER JOIN dim.Branch d ON a.BRANCH_NAME = d.BranchName 
--        INNER JOIN dim.State e ON a.STATE = e.State 
--        WHERE F.RowHash IS NULL;

--        SET @RowCount = @@ROWCOUNT; -- Update row count

--        IF @RowCount = 0
--        BEGIN
--            BREAK; -- Break the loop if no more rows are inserted
--        END
--    END
--END
--exec uspFact
---------------------FACT_BEGIN TRANSACTION,TRY,CATCH-----------------------------------------------

ALTER PROCEDURE uspFact
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        DECLARE @BatchSize INT = 50000; -- Specify your batch size
        DECLARE @RowCount INT = 1; -- Initialize the row count
		declare @ObjectName varchar(20),
		@StartDate datetime,
		@EndDate datetime,
		@NoOfRowsInserted int,
		@errormesssage varchar(300),
		@targeteff_date varchar(20)
		set @ObjectName='Fact.LoanDetails'
		set @StartDate=getdate()
        WHILE @RowCount > 0
        BEGIN
            INSERT INTO Fact.LOANDETAILS(
                Fk_BranchId,
                FK_CustomerId,
                FK_LoanID,
                FK_StateId,
                PrincipalTotal,
                PrinCollected,
                IntCollected,
                IntRate,
                ProcessingFee,
                Tenure,
                TOTALINTEREST,
                TOTALINSTAL,
                INSURANCECHARGES,
                OUTSTANDINGPRINCIPAL,
                OutstandingInterest,
                rowhash
            )
            SELECT TOP (@BatchSize)
                d.Pk_BranchId,
                b.pk_Customerid,
                c.Pk_LoanId,
                e.Pk_StateId,
                a.Principal_Total,
                a.PRIN_COLLECTED,
                a.INT_COLLECCTED,
                a.Int_Rate,
                a.Processing_Fee,
                a.Tenure,
                a.TOTAL_INTEREST,
                a.TOTAL_INSTAL,
                a.INSURANCE_CHARGES,
                a.OUTSTANDING_PRINCIPAL,
                a.Outstanding_Interest,
                a.Rowhash
            FROM STG.LoanStaging A
            LEFT JOIN Fact.LOANDETAILS F ON A.RowHash = F.Rowhash  
            INNER JOIN dim.Customer b ON a.Customer_ID = b.CustomerId AND b.Eff_Enddate IS NULL
            INNER JOIN dim.Loan c ON a.OLD_LOAN_NO = c.OldLoanNo AND c.Eff_Enddate IS NULL
            INNER JOIN dim.Branch d ON a.BRANCH_NAME = d.BranchName 
            INNER JOIN dim.State e ON a.STATE = e.State 
            WHERE F.RowHash IS NULL;

            SET @RowCount = @@ROWCOUNT; -- Update row count
            IF @RowCount = 0
            BEGIN
                BREAK; -- Break the loop if no more rows are inserted
            END
        END

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise error or log the error message
        DECLARE @ErrorMessage NVARCHAR(MAX);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
		
    END CATCH
	set @EndDate=getdate()
	insert into auditing (objectname,StartDate,EndDate,NoOfRowsInserted,errormesssage)values(@objectname,@startdate,@EndDate,@RowCount,@ErrorMessage)
	print(@rowcount)
END

select * from auditing
truncate table fact.loandetails
select * from fact.loandetails
---------------------------------------------------------------------------------------------------

EXEC uspFact
SELECT * FROM FACT.LOANDETAILS
ORDER BY FK_LoanID


------------------------------EXAMPLE FOR BEGIN TRANSACTIONS ,BEGIN TRY,BEGIN CATCH------------------------------------------------

CREATE PROCEDURE uspCustomers
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE INTO DIM.CUSTOMER AS TARGET
        USING (SELECT
                    DISTINCT CUSTOMER_ID ,
                    MAX(MEMBER_NAME) AS MEMBERNAME ,
                    MAX(DATE_OF_BIRTH) AS DATEOFBIRTH,
                    MAX(AGE) AS AGE
            FROM STG.STAGING
            GROUP BY CUSTOMER_ID
           ) AS SOURCE
        ON TARGET.CUSTOMERID=SOURCE.CUSTOMER_ID 
        WHEN MATCHED AND (SOURCE.MEMBERNAME<>TARGET.MEMBERNAME OR SOURCE.DATEOFBIRTH<>TARGET.DATEOFBIRTH OR SOURCE.AGE<>TARGET.AGE)
        --updating existing records--
        THEN UPDATE SET 
        TARGET.EFF_ENDDATE=GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
        INSERT (CUSTOMERID,MEMBERNAME,DATEOFBIRTH,AGE)
        VALUES(SOURCE.CUSTOMER_ID,SOURCE.MEMBERNAME,SOURCE.DATEOFBIRTH,SOURCE.AGE);


        --inserting new records--

        INSERT INTO DIM.CUSTOMER(CUSTOMERID,MEMBERNAME,DATEOFBIRTH,AGE)
        SELECT SOURCE.CUSTOMER_ID,SOURCE.MEMBER_NAME,SOURCE.DATE_OF_BIRTH,SOURCE.AGE
        FROM STG.LoanStaging source
        LEFT JOIN DIM.CUSTOMER b
        ON source.Customer_ID=b.customerid
        WHERE b.eff_enddate IS NOT NULL;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Raise error or log the error message
        DECLARE @ErrorMessage NVARCHAR(MAX);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT @ErrorMessage = ERROR_MESSAGE(),
               @ErrorSeverity = ERROR_SEVERITY(),
               @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;

exec uspCustomers













-----------------------------------
Alter proc Financial
as
begin

EXEC uspLoans
exec AuditLoan

exec   AuditCustomer
exec uspCustomers

EXEC USPSTATE
exec AuditState

exec AuditBranch
exec USPBRANCH

EXEC uspFact


end

exec Financial

select * from auditing

truncate table auditing