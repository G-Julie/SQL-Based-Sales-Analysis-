USE SalesDB
--Find the top highest sales for each product 
SELECT *
FROM (
	SELECT 
		OrderID,
		ProductID,
		Sales,
		RANK() OVER(PARTITION BY ProductID ORDER BY Sales DESC) SalesRank
	FROM Sales.Orders
	)t WHERE SalesRank = 1

--Calculate deviation of each sales from the minimum and maximum sales amounts
SELECT 
	*,
	Sales,
	MIN(Sales) OVER() MinkSales,
	MAX(Sales) OVER() MaxSales,
	Sales-MIN(Sales) OVER() DeviationFromMin,
	MAX(Sales) OVER() -Sales DeviationFromMax
FROM Sales.Orders

--Calculate the moving average of sales for each product over time
SELECT 
	OrderID,
	OrderDate,
	ProductID,
	Sales,
	AVG(Sales) OVER(PARTITION BY ProductID) AvgByProduct,
	AVG(Sales) OVER(PARTITION BY ProductID ORDER BY OrderDate) MovingAvg
FROM Sales.Orders

--Show the employees who have the highest salary
SELECT *
FROM (
SELECT 
	*,
	MAX(Salary) OVER() HighSal
FROM Sales.Employees
)t WHERE Salary=HighSal

--Find average score of customers, provide customer Id and lastname (check on Nulls)
SELECT
	CustomerID,
	LastName,
	COALESCE(Score,0),
	AVG(COALESCE(Score,0)) OVER() AvgScore
FROM Sales.Customers

--Calculate the moving average of sales for each product over time. Calculate the moving average of sales for each product over time, including only the next order
SELECT 
	OrderID,
	OrderDate,
	ProductID,
	Sales,
	AVG(Sales) OVER(PARTITION BY ProductID) AvgByProduct,
	AVG(Sales) OVER(PARTITION BY ProductID ORDER BY OrderDate) MovingAvg,
	AVG(Sales) OVER(PARTITION BY ProductID ORDER BY OrderDate ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) RollingAvg
FROM Sales.Orders

--Find the lowest 2 customers based on their total sales
SELECT *
FROM (
	SELECT 
		CustomerID,
		SUM(Sales) TotalSales,
		ROW_NUMBER() OVER(ORDER BY SUM(Sales)) CustomerRank
	FROM Sales.Orders
	GROUP BY CustomerID
	)t WHERE CustomerRank <=2

--Assign unique IDs to the rows of the ‘Order Archive’ table
SELECT 
	ROW_NUMBER() OVER(ORDER BY OrderID, OrderDate) UniqueID,
	*
FROM Sales.OrdersArchive

--Check whether the table ‘orders Archives’ contains any duplicate rows
SELECT *
FROM (
SELECT *,
	COUNT(*) OVER(PARTITION BY OrderID) tIP
FROM Sales.OrdersArchive
)t WHERE tIP > 1

--Find the percentage contribution of each product sales to the total sales
SELECT
	OrderID,
	OrderDate,
	Sales,
	SUM(Sales) OVER() TotalSalesByProduct,
	CAST (Sales AS FLOAT)/ SUM(Sales) OVER() * 100 PercentageSales 
FROM Sales.Orders

--Identify duplicate rows in the table ‘Order Archive’ and return a clean result without any duplicates
SELECT *
FROM (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY OrderID ORDER BY CreationTime DESC) rn,
		*
	FROM Sales.OrdersArchive
	)t WHERE rn=1

--Segment all orders into 3 categories: high, medium, low sales
SELECT *,
CASE WHEN Buckets =1 THEN 'High'
	WHEN Buckets =2 THEN 'Medium'
	WHEN Buckets =3 THEN 'Low'
END SalesSegmentations
FROM (
	SELECT
		OrderID,
		Sales,
		NTILE(3) OVER(ORDER BY Sales DESC) Buckets
	FROM Sales.Orders
)t

--Select the product that fall within the highest 40% of the prices
SELECT 
*,
CONCAT(DistRank * 100, '%') DistRankPerc
FROM (
	SELECT
	Product,
	Price,
	PERCENT_RANK() OVER(ORDER BY Price DESC) DistRank
	FROM Sales.Products
) t WHERE DistRank <= 0.4


--Analyze the month-over-month performance by finding the percentage change in sales between the current and previous month

USE SalesDB
SELECT 
*,
CurrentMonthSales-PreviousMonthSales AS MoM_Change,
CAST((CurrentMonthSales-PreviousMonthSales) AS FLOAT)/ PreviousMonthSales * 100
FROM (
	SELECT 
		MONTH (OrderDate) OrderMonth,
		SUM(Sales) CurrentMonthSales,
		LAG(SUM(Sales)) OVER(ORDER BY MONTH(OrderDate)) PreviousMonthSales
	FROM Sales.Orders
	GROUP BY MONTH(OrderDate)
 )t 


--In order to analyze customer loyalty, rank customers based on the average days between their orders
SELECT
CustomerID,
AVG(DaysUntilNextOrder) AvgDays,
RANK() OVER(ORDER BY COALESCE(AVG(DaysUntilNextOrder),99999))RankingAvg
FROM (
SELECT 
	OrderID,
	CustomerID,
	OrderDate CurrentOrder,
	LEAD(OrderDate) OVER(PARTITION BY CustomerID ORDER BY OrderDate) NextOrder,
	DATEDIFF(day,OrderDate,LEAD(OrderDate) OVER(PARTITION BY CustomerID ORDER BY OrderDate)) DaysUntilNextOrder   
FROM Sales.Orders
)t GROUP BY CustomerID


--Find the lowest and highest sales for each product
SELECT
	OrderID,
	ProductID,
	Sales,
	FIRST_VALUE(Sales) OVER(PARTITION BY ProductID ORDER BY Sales) LowestSales,
	LAST_VALUE(Sales) OVER(PARTITION BY ProductID ORDER BY Sales
	ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) HighestSales,
	FIRST_VALUE(Sales) OVER(PARTITION BY ProductID ORDER BY Sales DESC) LowestSales,
	MIN(Sales) OVER(PARTITION BY ProductID) LowestSales2,
	MAX(Sales) OVER(PARTITION BY ProductID) LowestSales3
FROM Sales.Orders

--Find the lowest and highest sales for each product and find the difference in sales between the current and the lowest sales
SELECT
	OrderID,
	ProductID,
	Sales,
	FIRST_VALUE(Sales) OVER(PARTITION BY ProductID ORDER BY Sales) LowestSales,
	LAST_VALUE(Sales) OVER(PARTITION BY ProductID ORDER BY Sales
	ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) HighestSales,
	Sales - FIRST_VALUE(Sales) OVER(PARTITION BY ProductID ORDER BY Sales) AS SalesDiff
FROM Sales.Orders


--Combine the data from employees and customers into one table
SELECT
	FirstName,
	LastName
FROM Sales.Customers
UNION
Select
	FirstName,
	LastName
FROM Sales.Employees

USE SalesDB

--Combine the data from employees and customers into one table including duplicates
SELECT
	FirstName,
	LastName
FROM Sales.Customers
UNION ALL
Select
	FirstName,
	LastName
FROM Sales.Employees

--Find employees who are not customers at the same time
SELECT
	FirstName,
	LastName
FROM Sales.Employees
EXCEPT
Select
	FirstName,
	LastName
FROM Sales.Customers

--Find employees who are also customers
SELECT
	FirstName,
	LastName
FROM Sales.Customers
INTERSECT
Select
	FirstName,
	LastName
FROM Sales.Employees

--Combine all orders data inot one report without duplicates
SELECT
'Orders' AS SourceTable
,[OrderID]
,[ProductID]
,[CustomerID]
,[SalesPersonID]
,[OrderDate]
,[ShipDate]
,[OrderStatus]
,[ShipAddress]
,[BillAddress]
,[Quantity]
,[Sales]
,[CreationTime]
FROM Sales.Orders
UNION
Select
'OrdersArchive' AS SourceTable
,[OrderID]
,[ProductID]
,[CustomerID]
,[SalesPersonID]
,[OrderDate]
,[ShipDate]
,[OrderStatus]
,[ShipAddress]
,[BillAddress]
,[Quantity]
,[Sales]
,[CreationTime]
FROM Sales.OrdersArchive


/*(Case When Statement)
Create report showing total sales for each of the following categories
:High (sales over 50) medium (sales 21-50), low (sales 20 or less)
sort categories from highest to lowest*/

SELECT
Category,
SUM(Sales) TotalSales
FROM (
	SELECT 
	OrderID,
	Sales,
	CASE
		WHEN Sales > 50 THEN 'High'
		WHEN Sales > 20 THEN 'Medium'
		ELSE 'Low'
	END Category
	FROM Sales.Orders
)t
GROUP BY Category
ORDER BY TotalSales DESC