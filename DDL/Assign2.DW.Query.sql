
--1, Which product generated maximum sales in Dec, 2014?
select p.product_name, sum(s.TOTAL_SALE) as product_total_sale from sales as s,products as p,dates as d
    where d.mm=12 and
          d.DATE_ID = s.DATE_ID and
          p.PRODUCT_ID = s.PRODUCT_ID
          group by p.product_name 
          order by product_total_sale desc
          FETCH FIRST 5 ROWS ONLY;

--2, Which store produced highest sales in the whole year?

--3, Determine the supplier name for the most popular product based on sales.

--4, Presents the quarterly sales analysis for all stores using drill down query concepts.

--5, Create a materialised view with name “STOREANALYSIS” that present the product-wisesales analysis for each store.