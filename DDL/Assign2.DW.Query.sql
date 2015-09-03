connect 'jdbc:derby://localhost:1527/countdownDW;user=kqc3001;password=password';

--1, Which product generated maximum sales in Dec, 2014?
select p.product_name, sum(s.TOTAL_SALE) as product_total_sale from sales as s,products as p,dates as d
    where d.mm=12 and
          d.DATE_ID = s.DATE_ID and
          p.PRODUCT_ID = s.PRODUCT_ID
          group by p.product_name 
          order by product_total_sale desc
          FETCH FIRST 3 ROWS ONLY;

--2, Which store produced highest sales in the whole year?
select s.STORE_NAME, sum(sales.TOTAL_SALE) store_sales from stores s, sales, dates d where 
    s.STORE_ID = sales.STORE_ID and 
    sales.DATE_ID = d.DATE_ID and
    d.YYYY = 2014
    group by s.STORE_NAME
    FETCH FIRST 3 ROWS ONLY;

--3, Determine the supplier name for the most popular product based on sales.
select s.SUPPLIER_NAME, sum(sales.TOTAL_SALE) supplier_sales from suppliers s, sales, products p where
    sales.SUPPLIER_ID = s.SUPPLIER_ID and 
    p.PRODUCT_ID = sales.PRODUCT_ID
    group by SUPPLIER_NAME
    FETCH FIRST 3 ROWS ONLY;

--4, Presents the quarterly sales analysis for all stores using drill down query concepts.

--5, Create a materialised view with name “STOREANALYSIS” that present the product-wise sales analysis for each store.

;