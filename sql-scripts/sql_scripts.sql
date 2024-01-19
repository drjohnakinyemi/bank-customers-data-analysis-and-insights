/**************************************************
Source: https://webpages.charlotte.edu/mirsad/itcs6265/group1/index.html

Project Goal
The Berka dataset, from the 1999 PKDD Discovery Challenge, provides information on the clients, accounts, and transactions of a Czech bank. 
The dataset deals with over 5,300 bank clients with approximately 1,000,000 transactions. 
Additionally, the bank represented in the dataset has extended close to 700 loans and issued nearly 900 credit cards, all of which are represented in the data.

The bank wants to improve their services. 
For instance, the bank managers have only vague idea, who is a good client (whom to offer some additional services) and who is a bad client (whom to watch carefully to minimize the bank loses). 

Fortunately, the bank stores data about their clients, the accounts (transactions within several months), the loans already granted, the credit cards issued. 

The bank managers hope to improve their understanding of customers and seek specific actions to improve services. 
A mere application of a discovery tool will not be convincing for them.

Our goal is to mine and analyze this bank data in order to extrapolate from it the type of customers who makes a good candidate for a credit card.
**************************************************/

USE BankDB;

/*
DATA IMPORT & RELATIONSHIPS
***************************
1. Import all 8 csv dataset into MS SQL Server.
2. Set primary and foreign keys.
3. Set relationships.

DATA PREPROCESSING
******************

4. Exploratory Data Analysis (EDA):
   Understand the dataset: data, structure, content, 
   and any potential issues with the dataset.

   * View a sample of data from each table.
*/

/* 
TODO: Rename tables with conflicting names to SQL keywords: 
 * Order to BankOrder
 * Transaction to BankTransaction
*/

EXEC sp_rename 'Order', BankOrder;
EXEC sp_rename 'Transaction', BankTransaction;

SELECT TOP(2) * FROM Account;
SELECT TOP(2) * FROM Client;
SELECT TOP(2) * FROM CreditCard;
SELECT TOP(2) * FROM Disposition;
SELECT TOP(2) * FROM District;
SELECT TOP(2) * FROM Loan;
SELECT TOP(2) * FROM BankOrder;
SELECT TOP(2) * FROM BankTransaction;

/* 
TODO: Transform dates into standard form on 4 tables: 
 * CreditCard. Field: IssueDate
 * Account. Field: EntryDate
 * Transaction. Field: EntryDate
 * Loan. Field: EntryDate
*/

UPDATE CreditCard SET IssuedDate = CONVERT(Date, IssuedDate);
UPDATE Account SET EntryDate = CONVERT(Date, EntryDate);
UPDATE Loan SET EntryDate = CONVERT(Date, EntryDate);
UPDATE BankTransaction SET EntryDate = CONVERT(Date, EntryDate);


/* 
5. DESCRIBE tables to view column names, data types, and constraints. 
   Correct/Convert Data Types.
*/

EXEC sp_help CreditCard;
EXEC sp_help Disposition;
EXEC sp_help Client;
EXEC sp_help District;
EXEC sp_help Account;
EXEC sp_help BankOrder;
EXEC sp_help BankTransaction;
EXEC sp_help Loan;

/* TODO: 
Client Table: Convert BirthNumber from string to integer
Account Table: Convert EntryDate to Date data type
CreditCard Table: Convert IssuedDate to Date data type.
Loan Table: 
	* Convert EntryDate to Date data type
	* Convert Amount from string to number
	* Convert Payments from string to number
	* Convert Duration from string to integer
BankOrder Table: Convert Amount from string to number
BankTransaction Table: 
	* Convert EntryDate to Date data type
	* Convert Amount from string to number
	* Convert Balance from string to number
*/

ALTER TABLE Client
ALTER COLUMN BirthNumber INT;

ALTER TABLE Account
ALTER COLUMN EntryDate DATE;

ALTER TABLE CreditCard
ALTER COLUMN IssuedDate DATE;

ALTER TABLE Loan
ALTER COLUMN EntryDate DATE;

ALTER TABLE Loan
ALTER COLUMN Amount DECIMAL(10,2);  -- 2 decimal places for Amount

ALTER TABLE Loan
ALTER COLUMN Payments DECIMAL(10,2);  -- 2 decimal places for Payments

ALTER TABLE Loan
ALTER COLUMN Duration INT;

ALTER TABLE BankOrder
ALTER COLUMN Amount DECIMAL(10,2);  -- 2 decimal places for Amount

ALTER TABLE BankTransaction
ALTER COLUMN EntryDate DATE;

ALTER TABLE BankTransaction
ALTER COLUMN Amount DECIMAL(10,2);  -- 2 decimal places for Amount

ALTER TABLE BankTransaction
ALTER COLUMN Balance DECIMAL(10,2);  -- 2 decimal places for Balance

/* 
6. Resolve missing values.
	* Identify missing values: 
        Determine if any values are missing (null) in a specific column, 
        which can affect data quality and analysis.
	* Use SELECT COUNT(*), COUNT(column_name) 

    * Understand the dataset: the data, data structure, 
	  content, and any potential issues with the dataset.
*/

SELECT COUNT(*) AS CountRows,
       COUNT(CardID) AS CountCardIDs,
       COUNT(Type) AS CountTypes,
       COUNT(IssuedDate) AS CountIssuedDates
FROM CreditCard;

SELECT COUNT(*) AS CountRows,
       COUNT(DispositionID) AS CountDispositionIDs,
       COUNT(ClientID) AS CountClientIDs,
       COUNT(Type) AS CountTypes
FROM Disposition;

SELECT COUNT(*) AS CountRows,
       COUNT(ClientID) AS CountClientIDs,
       COUNT(BirthNumber) AS CountBirthNumbers,
       COUNT(DistrictID) AS CountDistrictIDs
FROM Client;

SELECT COUNT(*) AS CountRows,
       COUNT(OrderID) AS CountOrderIDs,
       COUNT(AccountID) AS CountAccountIDs,
       COUNT(BankTo) AS CountBankTos,
       COUNT(AccountTo) AS CountAccountTos,
       COUNT(KSymbol) AS CountKSymbols,
       COUNT(Amount) AS CountAmounts
FROM BankOrder;

SELECT COUNT(*) AS CountRows,
       COUNT(LoanID) AS CountLoanIDs,
       COUNT(AccountID) AS CountAccountIDs,
       COUNT(EntryDate) AS CountEntryDates,
       COUNT(Amount) AS CountAmounts,
       COUNT(Duration) AS CountDurations,
       COUNT(Payments) AS CountPayments,
       COUNT(Status) AS CountStatuses
FROM Loan;

SELECT COUNT(*) AS CountRows,
       COUNT(DistrictID) AS CountDistrictIDs,
       COUNT(A2) AS CountA2Values,
       COUNT(A3) AS CountA3Values,
       COUNT(A4) AS CountA4Values,
       COUNT(A5) AS CountA5Values,
       COUNT(A6) AS CountA6Values,
       COUNT(A7) AS CountA7Values,
       COUNT(A8) AS CountA8Values,
       COUNT(A9) AS CountA9Values,
       COUNT(A10) AS CountA10Values,
       COUNT(A11) AS CountA11Values,
       COUNT(A12) AS CountA12Values,
       COUNT(A13) AS CountA13Values,
       COUNT(A14) AS CountA14Values,
       COUNT(A15) AS CountA15Values,
       COUNT(A16) AS CountA16Values
FROM District;

SELECT COUNT(*) AS CountRows,
       COUNT(AccountID) AS CountAccountIDs,
       COUNT(DistrictID) AS CountDistrictIDs,
       COUNT(Frequency) AS CountFrequencys,
       COUNT(EntryDate) AS CountEntryDates
FROM Account;

SELECT COUNT(*) AS CountRows,
       COUNT(TransactionID) AS CountTransactionIDs,
       COUNT(AccountID) AS CountAccountIDs,
       COUNT(EntryDate) AS CountEntryDates,
       COUNT(Type) AS CountTypes,
       COUNT(Operation) AS CountOperations,
       COUNT(Amount) AS CountAmounts,
       COUNT(Balance) AS CountBalances,
       COUNT(KSymbol) AS CountKSymbols,
       COUNT(Bank) AS CountBanks,
       COUNT(Account) AS CountAccounts
FROM BankTransaction;

/* TODO: Nothing. Data is good. */

/* Check for the uniqueness of primary keys to avoid duplicated primary key values.
	* Check for uniqueness: Verify if each value in a column is unique, 
        ensuring data integrity and preventing potential issues in operations or analysis that rely on unique identifiers.
	* This should be done with only primary key values.
	* Use SELECT COUNT(*), COUNT(DISTINCT column_name) 
*/

SELECT COUNT(*) AS CountRows,
       COUNT(DISTINCT CardID) AS CountUniqueCardIDs
FROM CreditCard;

SELECT COUNT(*) AS CountRows,
       COUNT(DISTINCT DispositionID) AS CountUniqueDispositionIDs
FROM Disposition;

SELECT COUNT(*) AS CountRows,
       COUNT(DISTINCT ClientID) AS CountUniqueClientIDs
FROM Client;

SELECT COUNT(*) AS CountRows,
       COUNT(DISTINCT OrderID) AS CountUniqueOrderIDs
FROM BankOrder;

SELECT COUNT(*) AS CountRows,
       COUNT(DISTINCT TransactionID) AS CountUniqueTransactionIDs
FROM BankTransaction;

SELECT COUNT(*) AS CountRows,
       COUNT(DISTINCT LoanID) AS CountUniqueLoanIDs
FROM Loan;

SELECT COUNT(*) AS CountRows,
       COUNT(DISTINCT DistrictID) AS CountUniqueDistrictIDs
FROM District;

SELECT COUNT(*) AS CountRows,
       COUNT(DISTINCT AccountID) AS CountUniqueAccountIDs
FROM Account;

/* TODO: Nothing. Data is good. */

/* Descriptive Stats: 
	* MIN, MAX, AVG to analyze descriptive statistics.
*/

SELECT MIN(CardID) AS MinCardID,
       MAX(CardID) AS MaxCardID,
       MIN(Type) AS MinType,
       MAX(Type) AS MaxType, 
       MIN(IssuedDate) AS MinIssuedDate,
       MAX(IssuedDate) AS MaxIssuedDate
FROM CreditCard;

SELECT MIN(DispositionID) AS MinDispositionID,
       MAX(DispositionID) AS MaxDispositionID,
       MIN(ClientID) AS MinClientID,
       MAX(ClientID) AS MaxClientID,
       MIN(Type) AS MinType,
       MAX(Type) AS MaxType
FROM Disposition;

SELECT MIN(ClientID) AS MinClientID,
       MAX(ClientID) AS MaxClientID,
       MIN(BirthNumber) AS MinBirthNumber,
       MAX(BirthNumber) AS MaxBirthNumber,
       MIN(DistrictID) AS MinDistrictID,
       MAX(DistrictID) AS MaxDistrictID
FROM Client;
/* Min DistrictID is 1, Max DistrictID is 9 */

SELECT MIN(OrderID) AS MinOrderID,
       MAX(OrderID) AS MaxOrderID,
       MIN(AccountID) AS MinAccountID,
       MAX(AccountID) AS MaxAccountID,
       MIN(BankTo) AS MinBankTo,
       MAX(BankTo) AS MaxBankTo,
       MIN(AccountTo) AS MinAccountTo,
       MAX(AccountTo) AS MaxAccountTo,
       MIN(KSymbol) AS MinKSymbol,
       MAX(KSymbol) AS MaxKSymbol,
       MIN(Amount) AS MinAmount,
       MAX(Amount) AS MaxAmount
FROM BankOrder;

SELECT MIN(TransactionID) AS MinTransactionID,
       MAX(TransactionID) AS MaxTransactionID,
       MIN(AccountID) AS MinAccountID,
       MAX(AccountID) AS MaxAccountID,
       MIN(EntryDate) AS MinEntryDate,
       MAX(EntryDate) AS MaxEntryDate,
       MIN(Type) AS MinType,
       MAX(Type) AS MaxType,
       MIN(Operation) AS MinOperation,
       MAX(Operation) AS MaxOperation,
       MIN(Amount) AS MinAmount,
       MAX(Amount) AS MaxAmount,
       MIN(Balance) AS MinBalance,
       MAX(Balance) AS MaxBalance,
       MIN(KSymbol) AS MinKSymbol,
       MAX(KSymbol) AS MaxKSymbol,
       MIN(Bank) AS MinBank,
       MAX(Bank) AS MaxBank,
       MIN(Account) AS MinAccount,
       MAX(Account) AS MaxAccount
FROM BankTransaction;

SELECT MIN(LoanID) AS MinLoanID,
       MAX(LoanID) AS MaxLoanID,
       MIN(AccountID) AS MinAccountID,
       MAX(AccountID) AS MaxAccountID,
       MIN(EntryDate) AS MinEntryDate,
       MAX(EntryDate) AS MaxEntryDate,
       MIN(Amount) AS MinAmount,
       MAX(Amount) AS MaxAmount,
       MIN(Duration) AS MinDuration,
       MAX(Duration) AS MaxDuration,
       MIN(Payments) AS MinPayments,
       MAX(Payments) AS MaxPayments,
       MIN(Status) AS MinStatus,
       MAX(Status) AS MaxStatus
FROM Loan;

SELECT MIN(DistrictID) AS MinDistrictID,
       MAX(DistrictID) AS MaxDistrictID,
       MIN(A2) AS MinA2,
       MAX(A2) AS MaxA2,
       MIN(A3) AS MinA3,
       MAX(A3) AS MaxA3,
       MIN(A4) AS MinA4,
       MAX(A4) AS MaxA4,
       MIN(A5) AS MinA5,
       MAX(A5) AS MaxA5,
       MIN(A6) AS MinA6,
       MAX(A6) AS MaxA6,
       MIN(A7) AS MinA7,
       MAX(A7) AS MaxA7,
       MIN(A8) AS MinA8,
       MAX(A8) AS MaxA8,
	   MIN(A9) AS MinA9,
       MAX(A9) AS MaxA9,
       MIN(A10) AS MinA10,
       MAX(A10) AS MaxA10,
       MIN(A11) AS MinA11,
       MAX(A11) AS MaxA11,
       MIN(A12) AS MinA12,
       MAX(A12) AS MaxA12,
       MIN(A13) AS MinA13,
       MAX(A13) AS MaxA13,
       MIN(A14) AS MinA14,
       MAX(A14) AS MaxA14,
       MIN(A15) AS MinA15,
       MAX(A15) AS MaxA15,
       MIN(A16) AS MinA16,
       MAX(A16) AS MaxA16
FROM District;

SELECT MIN(AccountID) AS MinAccountID,
       MAX(AccountID) AS MaxAccountID,
       MIN(DistrictID) AS MinDistrictID,
       MAX(DistrictID) AS MaxDistrictID,
       MIN(Frequency) AS MinFrequency,
       MAX(Frequency) AS MaxFrequency,
       MIN(EntryDate) AS MinEntryDate,
       MAX(EntryDate) AS MaxEntryDate
FROM Account;

/* TODO: Nothing */
/* Account: MIN EntryDate = 1993-01-01, MAX EntryDate = 1997-12-29 */
/* Loan: Min Status: A, Max Status: D */
/* BankTransaction; Min EntryDate = 1993-01-01, Max EntryDate = 1998-12-31 */



/* BankTransation table has an Operation field
	it provides a description of the type of transation done
	the field's values are difficult to easily process.

	VYBER KARTOU means Credit Card Withdrawal
	VKLAD means Cash Deposit
	PREVOD Z UCTU means Deposit from another bank
	VYBER means Cash Withdrawal
	PREVOD NA UCET means Remittance to another bank

	* We need a new table capture this information and 
		use shorter strings to represent the values as follows.

	WCC: VYBER KARTOU means Credit Card Withdrawal
	DC: VKLAD means Cash Deposit
	DB: PREVOD Z UCTU means Deposit from another bank
	WC: VYBER means Cash Withdrawal
	WB: PREVOD NA UCET means Remittance to another bank
*/

-- Create TransactionOperation table
CREATE TABLE TransactionOperation (
    OperationID VARCHAR(3) PRIMARY KEY,
    Description VARCHAR(50),
    OriginalCode VARCHAR(50)
);

-- Insert data into TransactionOperation
INSERT INTO TransactionOperation (OperationID, Description, OriginalCode)
VALUES
    ('WCC', 'Credit Card Withdrawal', 'VYBER KARTOU'),
    ('DC', 'Cash Deposit', 'VKLAD'),
    ('DB', 'Deposit from another bank', 'PREVOD Z UCTU'),
    ('WC', 'Cash Withdrawal', 'VYBER'),
    ('WB', 'Remittance to another bank', 'PREVOD NA UCET');

-- Add new column BankOperation to BankTransaction table
ALTER TABLE BankTransaction
ADD OperationID VARCHAR(3);

-- Alter BankTransaction table to add foreign key constraint
ALTER TABLE BankTransaction
ADD CONSTRAINT FK_BankTransaction_Operation
FOREIGN KEY (OperationID) REFERENCES TransactionOperation(OperationID);

-- Populate BankTransaction.OperationID based on TransactionOperation.OriginalCode
UPDATE BankTransaction
SET OperationID = TranOp.OperationID
FROM BankTransaction BT
JOIN TransactionOperation TranOp ON BT.Operation = TranOp.OriginalCode;

-- Drop the Operation column on BankTransaction table
ALTER TABLE BankTransaction
DROP COLUMN Operation;

-- Update values in BankTransaction table for easier comprehension
UPDATE BankTransaction
SET Type = 'Withdraw'
WHERE Type = 'VYDAJ'

UPDATE BankTransaction
SET Type = 'Deposit'
WHERE Type = 'PRIJEM'




/* Loan Operations 
	* Loan status represents the status of clients paying off their loans.
	* There are 4 states: A, B, C, D.
	* Create LoanStatus table as follows.
		LoanStatus:	
		StatusID (PK)	Description
		A	Contract finished, no problems
		B	Contract finished, loan not payed
		C	Running contract, OK thus-far
		D	Running contract, client in debt
	* There's going to be a PK-FK relationship with the Status column in the Loan table.
*/

-- Create LoanStatus table
CREATE TABLE LoanStatus (
    StatusID VARCHAR(1) PRIMARY KEY,
    Description VARCHAR(50)
);

-- Insert data into LoanStatus
INSERT INTO LoanStatus (StatusID, Description)
VALUES
    ('A', 'Contract finished, no problems'),
    ('B', 'Contract finished, loan not paid'),
    ('C', 'Running contract, OK thus-far'),
    ('D', 'Running contract, client in debt');

-- Add new column StatusID to Loan table
ALTER TABLE Loan
ADD StatusID VARCHAR(1);

-- Create PK-FK relationship between LoanStatus and Loan
ALTER TABLE Loan
ADD CONSTRAINT FK_Loan_Status
FOREIGN KEY (StatusID) REFERENCES LoanStatus(StatusID);

-- Update Loan.StatusID based on LoanStatus.StatusID
UPDATE Loan
SET StatusID = LS.StatusID
FROM Loan L
JOIN LoanStatus LS ON L.Status = LS.StatusID;

-- Drop the Status column from the Loan table
ALTER TABLE Loan
DROP COLUMN Status;








/**************************************************
ANALYZE DATA. ANSWER QUESTIONS.
**************************************************/

/*
-- Customer Segmentation with:
    * Segment Number, 
    * Average Amount, and 
    * List of AccountIDs
-- Identify different segments of customers based on their:
    * transaction behavior, and
    * account types.
*/

WITH CustomerSegments AS (
    SELECT AccountID,
        AVG(Amount) AS AvgTransactionAmount,
        NTILE(5) OVER (ORDER BY AVG(Amount)) AS Segment
    FROM BankTransaction
    GROUP BY AccountID
)
SELECT Segment,
    AVG(AvgTransactionAmount) AS AverageAmount,
    STRING_AGG(AccountID, ', ') AS AccountIDsInSegment
FROM CustomerSegments
GROUP BY Segment
ORDER BY Segment;


-- Districts with the most and least accounts + 
-- average number of accounts per district.
WITH DistrictAccountCounts AS (
    SELECT DistrictID,
        COUNT(DISTINCT AccountID) AS NumAccounts
    FROM Account
    GROUP BY DistrictID
)
SELECT
    (SELECT TOP 1 DistrictID FROM DistrictAccountCounts ORDER BY NumAccounts) AS DistrictWithMinAccounts,
    (SELECT MIN(NumAccounts) FROM DistrictAccountCounts) AS MinNoOfAccounts,
    (SELECT TOP 1 DistrictID FROM DistrictAccountCounts ORDER BY NumAccounts DESC) AS DistrictWithMaxAccounts,
    (SELECT MAX(NumAccounts) FROM DistrictAccountCounts) AS MaxNoOfAccounts,
    (SELECT AVG(NumAccounts * 1.0) FROM DistrictAccountCounts) AS AvgNoOfAccounts;


-- Districts with the highest and lowest loan payments + 
-- average loan payments per district.
WITH DistrictLoanPayments AS (
    SELECT
        A.DistrictID,
        SUM(L.Payments) AS TotalLoanPayments
    FROM
        Account A
    JOIN
        Loan L ON A.AccountID = L.AccountID
    GROUP BY
        A.DistrictID
)
SELECT
    -- District with the lowest loan payments
    (SELECT TOP 1 DistrictID FROM DistrictLoanPayments ORDER BY TotalLoanPayments) AS DistrictWithMinLoanPayments,
    
    -- Minimum loan payments in any district
    (SELECT MIN(TotalLoanPayments) FROM DistrictLoanPayments) AS MinLoanPayments,
    
    -- District with the highest loan payments
    (SELECT TOP 1 DistrictID FROM DistrictLoanPayments ORDER BY TotalLoanPayments DESC) AS DistrictWithMaxLoanPayments,
    
    -- Maximum loan payments in any district
    (SELECT MAX(TotalLoanPayments) FROM DistrictLoanPayments) AS MaxLoanPayments,
    
    -- Average loan payments per district
    (SELECT AVG(TotalLoanPayments * 1.0) FROM DistrictLoanPayments) AS AvgLoanPayments;



-- Accounts with the highest, lowest, and average transaction amounts.
WITH AccountTransactionAmounts AS (
    SELECT
        AccountID,
        MAX(Amount) AS HighestTransactionAmount,
        MIN(Amount) AS LowestTransactionAmount
    FROM
        BankTransaction
    GROUP BY
        AccountID
)
SELECT
    -- Account with the highest transaction amount
    (
        SELECT TOP 1 AccountID 
        FROM AccountTransactionAmounts 
        ORDER BY HighestTransactionAmount DESC) AS AccountWithMaxTransactionAmount,
    
    -- Highest transaction amount in any account
    (
        SELECT MAX(HighestTransactionAmount) 
        FROM AccountTransactionAmounts) AS MaxTransactionAmount,
    
    -- Account with the lowest transaction amount
    (
        SELECT TOP 1 AccountID 
        FROM AccountTransactionAmounts 
        ORDER BY LowestTransactionAmount) AS AccountWithMinTransactionAmount,
    
    -- Lowest transaction amount in any account
    (
        SELECT MIN(LowestTransactionAmount) 
        FROM AccountTransactionAmounts) AS MinTransactionAmount,
    
    -- Average transaction amount per account
    (
        SELECT AVG(HighestTransactionAmount * 1.0) 
        FROM AccountTransactionAmounts) AS AvgTransactionAmount;



-- Accounts with the highest, lowest, and average transaction frequencies.
WITH AccountTransactionFrequency AS (
    SELECT
        AccountID,
        COUNT(*) AS TransactionFrequency
    FROM
        BankTransaction
    GROUP BY
        AccountID
)
SELECT
    -- Account with the highest transaction frequency
    (SELECT TOP 1 AccountID FROM AccountTransactionFrequency ORDER BY TransactionFrequency DESC) AS AccountWithMaxTransactionFrequency,
    
    -- Highest transaction frequency in any account
    (SELECT MAX(TransactionFrequency) FROM AccountTransactionFrequency) AS MaxTransactionFrequency,
    
    -- Account with the lowest transaction frequency
    (SELECT TOP 1 AccountID FROM AccountTransactionFrequency ORDER BY TransactionFrequency) AS AccountWithMinTransactionFrequency,
    
    -- Lowest transaction frequency in any account
    (SELECT MIN(TransactionFrequency) FROM AccountTransactionFrequency) AS MinTransactionFrequency,
    
    -- Average transaction frequency per account
    (SELECT AVG(TransactionFrequency * 1.0) FROM AccountTransactionFrequency) AS AvgTransactionFrequency;


/* High-Value Customers: 
    * Who are our high-value customers, and what are their characteristics?
    -- A high value account has tan amount in the bank > average sum of all amounts.
    -- High-value clients based on various criteria.
*/

WITH HighValueClients AS (
    SELECT DISTINCT D.ClientID, SUM(BO.Amount) AS HighValueClientByBankOrderAmount
    FROM BankOrder BO
    JOIN Account A ON BO.AccountID = A.AccountID
    JOIN Disposition D ON A.AccountID = D.AccountID
    GROUP BY D.ClientID
),
HighValueClientsByLoan AS (
    SELECT DISTINCT D.ClientID, SUM(L.Amount) AS HighValueClientByLoanAmount
    FROM Loan L
    JOIN Account A ON L.AccountID = A.AccountID
    JOIN Disposition D ON A.AccountID = D.AccountID
    GROUP BY D.ClientID
),
HighValueClientsByTransaction AS (
    SELECT DISTINCT D.ClientID, SUM(BT.Amount) AS HighValueClientByBankTransactionAmount
    FROM BankTransaction BT
    JOIN Account A ON BT.AccountID = A.AccountID
    JOIN Disposition D ON A.AccountID = D.AccountID
    GROUP BY D.ClientID
)
-- Combine results using outer joins.
SELECT
    COALESCE(HVC.ClientID, HVL.ClientID, HVBT.ClientID) AS ClientID,
    HVC.HighValueClientByBankOrderAmount,
    HVL.HighValueClientByLoanAmount,
    HVBT.HighValueClientByBankTransactionAmount
FROM
    HighValueClients HVC
FULL OUTER JOIN
    HighValueClientsByLoan HVL ON HVC.ClientID = HVL.ClientID
FULL OUTER JOIN
    HighValueClientsByTransaction HVBT ON HVC.ClientID = HVBT.ClientID;




-- Group loans based on StatusID and calculate count, sum, and average
SELECT StatusID, 
		COUNT(*) AS LoanCount, 
		SUM(Amount) AS TotalAmount, 
		AVG(Amount) AS AverageAmount, 
		MIN(Amount) AS MinAmount, 
		MAX(Amount) AS MaxAmount
FROM Loan
GROUP BY StatusID;

-- Loan Stats per Year. 
-- Group loans based on StatusID and EntryYear, and 
-- calculate count, sum, average, min, and max
SELECT
    StatusID,
    YEAR(EntryDate) AS EntryYear,
    COUNT(*) AS LoanCount,
    SUM(Amount) AS TotalAmount,
    AVG(Amount) AS AverageAmount,
    MIN(Amount) AS MinAmount,
    MAX(Amount) AS MaxAmount
FROM
    Loan
GROUP BY
    StatusID,
    YEAR(EntryDate);

-- Loan Stats per Month. 
-- Group loans based on StatusID, Year, and Month, and 
-- calculate count, sum, average, min, and max
SELECT
    StatusID,
    YEAR(EntryDate) AS EntryYear,
    MONTH(EntryDate) AS EntryMonth,
    COUNT(*) AS LoanCount,
    SUM(Amount) AS TotalAmount,
    AVG(Amount) AS AverageAmount,
    MIN(Amount) AS MinAmount,
    MAX(Amount) AS MaxAmount
FROM Loan
GROUP BY
    StatusID,
    YEAR(EntryDate),
    MONTH(EntryDate);


-- Cross-reference Duration and StatusID from the Loan table, 
-- count and include additional stats on loan amount (formatted to 2 decimal places), and sort
SELECT
    Duration,
    StatusID,
    COUNT(*) AS LoanCount,
    ROUND(MAX(Amount), 2) AS MaxAmount,
    ROUND(MIN(Amount), 2) AS MinAmount,
    ROUND(SUM(Amount), 2) AS TotalAmount,
    ROUND(AVG(Amount), 2) AS AverageAmount
FROM
    Loan
GROUP BY
    Duration,
    StatusID
ORDER BY
    Duration ASC,
    StatusID ASC;

-- Cross-reference Year, Month, Duration, and StatusID from the Loan table, 
-- count and include additional stats on loan amount (formatted to 2 decimal places), and sort
SELECT
    YEAR(EntryDate) AS LoanYear,
    MONTH(EntryDate) AS LoanMonth,
    Duration,
    StatusID,
    COUNT(*) AS LoanCount,
    ROUND(MAX(Amount), 2) AS MaxAmount,
    ROUND(MIN(Amount), 2) AS MinAmount,
    ROUND(SUM(Amount), 2) AS TotalAmount,
    ROUND(AVG(Amount), 2) AS AverageAmount
FROM
    Loan
GROUP BY
    YEAR(EntryDate),
    MONTH(EntryDate),
    Duration,
    StatusID
ORDER BY
    LoanYear ASC,
    LoanMonth ASC,
    Duration ASC,
    StatusID ASC;


-- Cross-reference Year, Duration, and StatusID from the Loan table, 
-- count and include additional stats on loan amount (formatted to 2 decimal places), and sort
SELECT
    YEAR(EntryDate) AS LoanYear,
    Duration,
    StatusID,
    COUNT(*) AS LoanCount,
    ROUND(MAX(Amount), 2) AS MaxAmount,
    ROUND(MIN(Amount), 2) AS MinAmount,
    ROUND(SUM(Amount), 2) AS TotalAmount,
    ROUND(AVG(Amount), 2) AS AverageAmount
FROM
    Loan
GROUP BY
    YEAR(EntryDate),
    Duration,
    StatusID
ORDER BY
    LoanYear ASC,
    Duration ASC,
    StatusID ASC;


-- Cross-reference Year, District, Duration, and StatusID from the Loan table, 
-- count and include additional stats on loan amount (formatted to 2 decimal places), and sort
SELECT
    YEAR(L.EntryDate) AS LoanYear,
    D.DistrictID,
    Duration,
    StatusID,
    COUNT(*) AS LoanCount,
    ROUND(MAX(L.Amount), 2) AS MaxAmount,
    ROUND(MIN(L.Amount), 2) AS MinAmount,
    ROUND(SUM(L.Amount), 2) AS TotalAmount,
    ROUND(AVG(L.Amount), 2) AS AverageAmount
FROM
    Loan L
JOIN
    Account A ON L.AccountID = A.AccountID
JOIN
    District D ON A.DistrictID = D.DistrictID
GROUP BY
    YEAR(L.EntryDate),
    D.DistrictID,
    Duration,
    StatusID
ORDER BY
    LoanYear ASC,
    D.DistrictID ASC,
    Duration ASC,
    StatusID ASC;


-- Cross-reference Year, Month, District, Duration, and StatusID from the Loan table, 
-- count and include additional stats on loan amount (formatted to 2 decimal places), and sort
SELECT
    YEAR(L.EntryDate) AS LoanYear,
    MONTH(L.EntryDate) AS LoanMonth,
    D.DistrictID,
    Duration,
    StatusID,
    COUNT(*) AS LoanCount,
    ROUND(MAX(L.Amount), 2) AS MaxAmount,
    ROUND(MIN(L.Amount), 2) AS MinAmount,
    ROUND(SUM(L.Amount), 2) AS TotalAmount,
    ROUND(AVG(L.Amount), 2) AS AverageAmount
FROM
    Loan L
JOIN
    Account A ON L.AccountID = A.AccountID
JOIN
    District D ON A.DistrictID = D.DistrictID
GROUP BY
    YEAR(L.EntryDate),
    MONTH(L.EntryDate),
    D.DistrictID,
    Duration,
    StatusID
ORDER BY
    LoanYear ASC,
    LoanMonth ASC,
    D.DistrictID ASC,
    Duration ASC,
    StatusID ASC;




-- CROSS JOIN to include months when there's no amount entered. 
-- Cross-reference Year, Month, District, Duration, and StatusID from the Loan table,
-- count and include additional stats on loan amount (formatted to 2 decimal places),
-- fill in values for months with Amount as 0, and sort

WITH YearList AS (
    SELECT DISTINCT YEAR(EntryDate) AS Year FROM Loan
),
MonthList AS (
    SELECT DISTINCT MONTH(EntryDate) AS Month FROM Loan
)
SELECT
    Y.Year,
    M.Month,
    D.DistrictID,
    L.Duration,
    LS.StatusID,
    COUNT(L.LoanID) AS LoanCount,
    COALESCE(ROUND(MAX(L.Amount), 2), 0) AS MaxAmount,
    COALESCE(ROUND(MIN(L.Amount), 2), 0) AS MinAmount,
    COALESCE(ROUND(SUM(L.Amount), 2), 0) AS TotalAmount,
    COALESCE(ROUND(AVG(L.Amount), 2), 0) AS AverageAmount
FROM
    YearList Y
CROSS JOIN
    MonthList M
CROSS JOIN
    District D
CROSS JOIN
    LoanStatus LS
LEFT JOIN
    Loan L ON Y.Year = YEAR(L.EntryDate) AND M.Month = MONTH(L.EntryDate) AND LS.StatusID = L.StatusID
LEFT JOIN
    Account A ON L.AccountID = A.AccountID
LEFT JOIN
    District DD ON A.DistrictID = DD.DistrictID
WHERE
    L.Duration IS NOT NULL
GROUP BY
    Y.Year,
    M.Month,
    D.DistrictID,
    L.Duration,
    LS.StatusID
ORDER BY
    Y.Year ASC,
    M.Month ASC,
    D.DistrictID ASC,
    L.Duration ASC,
    LS.StatusID ASC;




-- Effect of seasons on Account Opening.
-- Are there seasonal trends in client account opening habits?

SELECT
    YEAR(EntryDate) AS Year,
    MONTH(EntryDate) AS Month,
    COUNT(DISTINCT AccountID) AS NumAccountsOpened
FROM
    Account
GROUP BY
    YEAR(EntryDate),
	MONTH(EntryDate)
ORDER BY
    YEAR(EntryDate),
	MONTH(EntryDate);

-- Seasonal trends in client account opening habits per district.
SELECT
    YEAR(EntryDate) AS Year,
    MONTH(EntryDate) AS Month,
    DistrictID,
    COUNT(DISTINCT AccountID) AS NumAccountsOpened
FROM
    Account
GROUP BY
    YEAR(EntryDate),
    MONTH(EntryDate),
    DistrictID
ORDER BY
    YEAR(EntryDate),
    MONTH(EntryDate),
    DistrictID;


-- Effect of seasons on Loan Payments.
-- Are there seasonal trends in client loan payment habits?
-- Loan payment trends per month and year.

SELECT
    YEAR(EntryDate) AS Year,
    MONTH(EntryDate) AS Month,
    COUNT(DISTINCT LoanID) AS NumLoans,
    COUNT(*) AS TotalPayments,
    SUM(Payments) AS TotalPaymentAmount,
    AVG(Payments * 1.0) AS AvgPaymentAmount,
    MAX(Payments) AS MaxPaymentAmount,
    MIN(Payments) AS MinPaymentAmount
FROM
    Loan
GROUP BY
    YEAR(EntryDate),
    MONTH(EntryDate)
ORDER BY
    YEAR(EntryDate),
    MONTH(EntryDate);

-- Seasonal trends in client loan payment habits per district.
SELECT
    YEAR(L.EntryDate) AS Year,
    MONTH(L.EntryDate) AS Month,
    A.DistrictID,
    COUNT(DISTINCT L.LoanID) AS NumLoans,
    COUNT(*) AS TotalPayments,
    SUM(L.Payments) AS TotalPaymentAmount,
    AVG(L.Payments * 1.0) AS AvgPaymentAmount,
    MAX(L.Payments) AS MaxPaymentAmount,
    MIN(L.Payments) AS MinPaymentAmount
FROM
    Loan L
JOIN
    Account A ON L.AccountID = A.AccountID
GROUP BY
    YEAR(L.EntryDate),
    MONTH(L.EntryDate),
    A.DistrictID
ORDER BY
    YEAR(L.EntryDate),
    MONTH(L.EntryDate),
    A.DistrictID;




-- Effect of seasons on Loan Status.
-- Are there seasonal trends in client loan status habits?
-- Loan status trends per month and year.

SELECT
    Y.Year,
    M.Month,
    LS.StatusID,
    COALESCE(COUNT(DISTINCT L.LoanID), 0) AS NumLoans
FROM
    (SELECT DISTINCT YEAR(EntryDate) AS Year FROM Loan) Y
CROSS JOIN
    (SELECT DISTINCT MONTH(EntryDate) AS Month FROM Loan) M
CROSS JOIN
    LoanStatus LS
LEFT JOIN
    Loan L ON Y.Year = YEAR(L.EntryDate) AND M.Month = MONTH(L.EntryDate) AND LS.StatusID = L.StatusID
GROUP BY
    Y.Year,
    M.Month,
    LS.StatusID
ORDER BY
    Y.Year,
    M.Month,
    LS.StatusID;

-- Seasonal trends in client status habits per district.
SELECT
    Y.Year,
    M.Month,
    A.DistrictID,
    LS.StatusID,
    COALESCE(COUNT(DISTINCT L.LoanID), 0) AS NumLoans
FROM
    (SELECT DISTINCT YEAR(EntryDate) AS Year FROM Loan) Y
CROSS JOIN
    (SELECT DISTINCT MONTH(EntryDate) AS Month FROM Loan) M
CROSS JOIN
    LoanStatus LS
CROSS JOIN
    Account A
LEFT JOIN
    Loan L ON Y.Year = YEAR(L.EntryDate) AND M.Month = MONTH(L.EntryDate) AND LS.StatusID = L.StatusID
    AND A.AccountID = L.AccountID
GROUP BY
    Y.Year,
    M.Month,
    A.DistrictID,
    LS.StatusID
ORDER BY
    Y.Year,
    M.Month,
    A.DistrictID,
    LS.StatusID;




-- Effect of seasons on Bank Transactions.
-- Are there seasonal trends in client bank transaction habits?
-- We want to gain insights into the effect of seasons on Account Opening.
SELECT
    YEAR(EntryDate) AS Year,
    MONTH(EntryDate) AS Month,
    MIN(Amount) AS MinAmount,
    MAX(Amount) AS MaxAmount,
    SUM(Amount) AS TotalAmount,
    AVG(Amount * 1.0) AS AvgAmount,
    COUNT(*) AS NumTransactions
FROM
    BankTransaction
GROUP BY
    YEAR(EntryDate),
    MONTH(EntryDate)
ORDER BY
    YEAR(EntryDate),
    MONTH(EntryDate);


-- Seasonal trends of bank transaction habits per district.
SELECT
    YEAR(BT.EntryDate) AS Year,
    MONTH(BT.EntryDate) AS Month,
    A.DistrictID,
    MIN(BT.Amount) AS MinAmount,
    MAX(BT.Amount) AS MaxAmount,
    SUM(BT.Amount) AS TotalAmount,
    AVG(BT.Amount * 1.0) AS AvgAmount,
    COUNT(*) AS NumTransactions
FROM
    BankTransaction BT
JOIN
    Account A ON BT.AccountID = A.AccountID
GROUP BY
    YEAR(BT.EntryDate),
    MONTH(BT.EntryDate),
    A.DistrictID
ORDER BY
    YEAR(BT.EntryDate),
    MONTH(BT.EntryDate),
    A.DistrictID;




-- What banks do we transact with?
SELECT
    Bank,
    COUNT(*) AS NumTransactions,
    SUM(Amount) AS TotalAmount,
    AVG(Amount * 1.0) AS AvgAmount,
    MIN(Amount) AS MinAmount,
    MAX(Amount) AS MaxAmount
FROM
    BankTransaction
WHERE
    Bank IS NOT NULL AND Bank <> ''
GROUP BY
    Bank
ORDER BY
    Bank;


-- What type of transactions do we do with the banks?
SELECT
    Bank,
    Type,
    COUNT(*) AS NumTransactions,
    SUM(Amount) AS TotalAmount,
    AVG(Amount * 1.0) AS AvgAmount,
    MIN(Amount) AS MinAmount,
    MAX(Amount) AS MaxAmount
FROM
    BankTransaction
WHERE
    Bank IS NOT NULL AND Bank <> '' AND Type IS NOT NULL AND Type <> ''
GROUP BY
    Bank, Type
ORDER BY
    Bank, Type;

