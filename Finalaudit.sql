
--DECLARE @MergeAction TABLE (ActionTaken VARCHAR(10));
--Declare @objectname varchar(30),
--@startdate datetime,
--@enddate datetime,
--@errormessage varchar(20)
--begin try
--set @objectname='Dim.Customer'
--set @startdate=getdate()
--MERGE INTO DIM.CUSTOMER AS TARGET
--USING (
--    SELECT DISTINCT
--        CUSTOMER_ID,
--        MAX(MEMBER_NAME) AS MEMBERNAME,
--        MAX(DATE_OF_BIRTH) AS DATEOFBIRTH,
--        MAX(AGE) AS AGE
--    FROM STG.LoanSTAGING
--    GROUP BY CUSTOMER_ID
--) AS SOURCE
--ON TARGET.CUSTOMERID = SOURCE.CUSTOMER_ID
--WHEN MATCHED AND (SOURCE.MEMBERNAME <> TARGET.MEMBERNAME OR SOURCE.DATEOFBIRTH <> TARGET.DATEOFBIRTH OR SOURCE.AGE <> TARGET.AGE) THEN
--    UPDATE SET TARGET.EFF_ENDDATE = GETDATE()
--WHEN NOT MATCHED BY TARGET THEN
--    INSERT (CUSTOMERID, MEMBERNAME, DATEOFBIRTH, AGE)
--    VALUES (SOURCE.CUSTOMER_ID, SOURCE.MEMBERNAME, SOURCE.DATEOFBIRTH, SOURCE.AGE)
--OUTPUT $action INTO @MergeAction;
-- INSERT INTO DIM.CUSTOMER(CUSTOMERID,MEMBERNAME,DATEOFBIRTH,AGE)
--        SELECT SOURCE.CUSTOMER_ID,SOURCE.MEMBER_NAME,SOURCE.DATE_OF_BIRTH,SOURCE.AGE
--        FROM STG.LoanStaging source
--        LEFT JOIN DIM.CUSTOMER b
--        ON source.Customer_ID=b.customerid
--        WHERE b.eff_enddate IS NOT NULL;
 
--DECLARE @UpdatedCount INT = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'UPDATE');
--DECLARE @InsertedCount INT = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'INSERT');
--set @enddate=getdate()
--SELECT @UpdatedCount AS UpdatedCount, @InsertedCount AS InsertedCount;
--end try
--begin  catch
--set @errormessage=ERROR_MESSAGE()
--end catch
--insert into auditing values(@objectname,@startdate,@enddate,@UpdatedCount,@InsertedCount,@errormessage)
--select * from auditing
-- truncate table auditing
-- truncate table dim.customer
 
 

--DECLARE @MergeAction TABLE (ActionTaken VARCHAR(10));
--Declare @objectname varchar(30),
--@startdate datetime,
--@enddate datetime,
--@errormessage varchar(20)
--begin try
--set @objectname='Dim.Loan'
--set @startdate=getdate()
-- MERGE DIM.LOAN AS target
--USING (
--       SELECT DISTINCT
--						Old_Loan_No,
--						PURPOSE_ID,
--						DISBURSEMENT_DATE,--convert(datetime,DISBURSEMENT_DATE,101)as DISBURSEMENT_DATE,
--						LOAN_CREATED_ON,--convert(datetime,LOAN_CREATED_ON,101)as LOAN_CREATED_ON,
--						LAST_MATURITY_DATE,--convert(datetime,LAST_MATURITY_DATE,101)as LAST_MATURITY_DATE,
--						STATUS,
--						LOAN_CLOSURE_DATE,--convert(datetime,LOAN_CLOSURE_DATE,101)as LOAN_CLOSURE_DATE,
--						INSURANCE_MATURITY_DATE--convert(datetime,INSURANCE_MATURITY_DATE,101)as INSURANCE_MATURITY_DATE
--    FROM STG.LoanSTAGING
--) AS source
--ON target.OldLoanNo = source.Old_Loan_NO-- and target.status<>source.status or 
----target.LastMaturityDate<>source.Last_Maturity_Date or target.LoanClosureDate<>source.Loan_Closure_Date or 
----target.InsuranceMaturityDate<>source.Insurance_Maturity_Date 

--WHEN MATCHED  and (target.status<>source.status or 
--target.LastMaturityDate<>source.Last_Maturity_Date or target.LoanClosureDate<>source.Loan_Closure_Date or 
--target.InsuranceMaturityDate<>source.Insurance_Maturity_Date or target.purposeid<>source.purpose_id )
--THEN
--    UPDATE SET
--		target.Eff_EndDate=getdate()
		

--when NOT MATCHED BY TARGET THEN
--    INSERT (
--				OldLoanNo,
--				PurposeId,
--				DisbursementDate,
--				LoanCreatedOn,
--				LastMaturityDate,
--				Status,
--				LoanClosureDate,
--				InsuranceMaturityDate
--		   ) 
--	VALUES (
--				source.Old_Loan_NO,
--				source.PURPOSE_ID,
--				source.DISBURSEMENT_DATE,
--				source.LOAN_CREATED_ON,
--				source.LAST_MATURITY_DATE,
--				source.STATUS,
--				source.LOAN_CLOSURE_DATE,
--				source.INSURANCE_MATURITY_DATE
--    )

--	--inserting the updated records into loandimension table using left join
--	OUTPUT $action INTO @MergeAction;
--	insert into Dim.Loan(  
--							OldLoanNo,
--							PurposeId,
--							DisbursementDate,
--							LoanCreatedOn,
--							LastMaturityDate,
--							Status,
--							LoanClosureDate,
--							InsuranceMaturityDate
--						 )
--				select 
--							source.Old_Loan_NO,
--							source.PURPOSE_ID,
--							source.DISBURSEMENT_DATE,
--							source.LOAN_CREATED_ON,
--							source.LAST_MATURITY_DATE,
--							source.STATUS,
--							source.LOAN_CLOSURE_DATE,
--							source.INSURANCE_MATURITY_DATE
--	from STG.LoanStaging source
--	left join dim.Loan target
--	on target.OldLoanNo=source.old_loan_no
--	where target.EFF_ENDDATE is not null
--DECLARE @UpdatedCount INT = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'UPDATE');
--DECLARE @InsertedCount INT = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'INSERT');
--set @enddate=getdate()
--SELECT @UpdatedCount AS UpdatedCount, @InsertedCount AS InsertedCount;
--end try
--begin  catch
--set @errormessage=ERROR_MESSAGE()
--end catch  
--insert into auditing values(@objectname,@startdate,@enddate,@UpdatedCount,@InsertedCount,@errormessage)
--SELECT * FROM auditing
alter proc Project
as
begin
DECLARE @MergeAction TABLE (ActionTaken VARCHAR(10));
Declare @objectname varchar(30),
@startdate datetime,
@enddate datetime,
@errormessage varchar(20),
@InsertedCount int,
@UpdatedCount int
begin try
set @objectname='Dim.State'
set @startdate=getdate()
MERGE INTO DIM.STATE AS TARGET
USING (
		SELECT DISTINCT stg.STATE
		FROM STG.LoanSTAGING  stg
		LEFT OUTER JOIN DIM.STATE S
		ON STG.state=S.STATE
		where s.state is null 
		)
 AS SOURCE
ON SOURCE.STATE=TARGET.STATE
WHEN MATCHED AND SOURCE.STATE<>TARGET.STATE 
THEN
UPDATE SET target.eff_enddate=getdate()
WHEN NOT MATCHED BY TARGET THEN
INSERT(STATE)
VALUES(SOURCE.STATE)
OUTPUT $action INTO @MergeAction;
INSERT into dim.state(STATE)
select stg.state
FROM STG.LoanStaging  STG
		LEFT OUTER JOIN DIM.STATE S
		ON STG.state=S.STATE
		where s.state is null and s.EFF_ENDDATE is null
set @UpdatedCount  = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'UPDATE');
set @InsertedCount  = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'INSERT');
set @enddate=getdate()
SELECT @UpdatedCount AS UpdatedCount, @InsertedCount AS InsertedCount;
end try
begin  catch
set @errormessage=ERROR_MESSAGE()
end catch  
insert into auditing values(@objectname,@startdate,@enddate,@InsertedCount,@UpdatedCount,@errormessage)


begin try
set @objectname='Dim.Branch'
set @startdate=getdate()
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
VALUES(SOURCE.BRANCH_NAME)
OUTPUT $action INTO @MergeAction;
insert into dim.BRANCH(BRANCHNAME)
select stg.branch_name from stg.LoanStaging stg
left outer join dim.BRANCH b
on stg.branch_name=b.BRANCHNAME
where b.BRANCHNAME is null and EFF_ENDDATE is null
set @UpdatedCount  = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'UPDATE');
set @InsertedCount  = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'INSERT');
set @enddate=getdate()
SELECT @UpdatedCount AS UpdatedCount, @InsertedCount AS InsertedCount;
end try
begin  catch
set @errormessage=ERROR_MESSAGE()
end catch  
insert into auditing values(@objectname,@startdate,@enddate,@InsertedCount,@UpdatedCount,@errormessage)


--update stg.LoanStaging
--set 
--state='up '
--where state='uttarpradesh'


begin try
set @objectname='Dim.Customer'
set @startdate=getdate()
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
        VALUES(SOURCE.CUSTOMER_ID,SOURCE.MEMBERNAME,SOURCE.DATEOFBIRTH,SOURCE.AGE)


OUTPUT $action INTO @MergeAction;
INSERT INTO DIM.CUSTOMER(CUSTOMERID,MEMBERNAME,DATEOFBIRTH,AGE)
        SELECT
                    DISTINCT CUSTOMER_ID ,
                    MAX(MEMBER_NAME) AS MEMBERNAME ,
                    MAX(DATE_OF_BIRTH) AS DATEOFBIRTH,
                    MAX(stg.AGE) AS AGE
            FROM STG.LoanSTAGING stg
			left outer join dim.customer  d
			on stg.customer_id=d.CUSTOMERID
			where d.EFF_ENDDATE is not null
            GROUP BY CUSTOMER_ID
set @UpdatedCount = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'UPDATE');
set @InsertedCount  = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'INSERT');
set @enddate=getdate()
SELECT @UpdatedCount AS UpdatedCount, @InsertedCount AS InsertedCount;
end try
begin  catch
set @errormessage=ERROR_MESSAGE()
end catch  
insert into auditing values(@objectname,@startdate,@enddate,@InsertedCount,@UpdatedCount,@errormessage)

 

begin try
set @objectname='Dim.Loan'
set @startdate=getdate()

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
    )
	OUTPUT $action INTO @MergeAction;
	

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
set @UpdatedCount = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'UPDATE');
set @InsertedCount  = (SELECT COUNT(*) FROM @MergeAction WHERE ActionTaken = 'INSERT');
set @enddate=getdate()
SELECT @UpdatedCount AS UpdatedCount, @InsertedCount AS InsertedCount;
end try
begin  catch
set @errormessage=ERROR_MESSAGE()
end catch  
insert into auditing values(@objectname,@startdate,@enddate,@InsertedCount,@UpdatedCount,@errormessage)
select * from   auditing
end
exec project
select * from auditing
-----------------------------fact.loan------------------------------------------------------
ALTER PROCEDURE uspFact
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
		DECLARE @MergeAction TABLE (ActionTaken VARCHAR(10));
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
			SET @NoOfRowsInserted=(select count(*) from fact.LoanDetails)
            IF @RowCount = 0
            BEGIN
                BREAK; -- Break the loop if no more rows are inserted
            END
        END

        COMMIT TRANSACTION;
			insert into auditing (objectname,StartDate,EndDate,NoOfRowsInserted,errormesssage)values(@objectname,@startdate,@EndDate,@NoOfRowsInserted,@ErrorMessage)

    END TRY
    BEGIN CATCH
	      print(@@TRANCOUNT)
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
	set @EndDate=getdate()
	print(@rowcount)
END

exec uspFact

select * from   auditing

select * from fact.LOANDETAILS

truncate table fact.loandetails