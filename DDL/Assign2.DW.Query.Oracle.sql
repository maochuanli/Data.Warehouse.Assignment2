--1, Which product generated maximum sales in Dec, 2014?
select * from  (  
  select products.product_name, sum(sales.TOTAL_SALE) as product_total_sale from sales, products, dates
    where dates.mm=12 and
          dates.DATE_ID = sales.DATE_ID and
          products.PRODUCT_ID = sales.PRODUCT_ID
          group by products.product_name 
          order by product_total_sale desc          
    
) where ROWNUM <= 3;

--2, Which store produced highest sales in the whole year?
select * from  (  
  select s.STORE_NAME, sum(sales.TOTAL_SALE) store_sales from stores s, sales, dates d where 
    s.STORE_ID = sales.STORE_ID and 
    sales.DATE_ID = d.DATE_ID and
    d.YYYY = 2014 
    group by s.STORE_NAME
    
) where ROWNUM <= 3;

--3, Determine the supplier name for the most popular product based on sales.
select * from  (  
  select s.SUPPLIER_NAME, sum(sales.TOTAL_SALE) supplier_sales from suppliers s, sales, products p where
    sales.SUPPLIER_ID = s.SUPPLIER_ID and 
    p.PRODUCT_ID = sales.PRODUCT_ID     
    group by SUPPLIER_NAME
    
) where ROWNUM <= 3;

--4, Presents the quarterly sales analysis for all stores using drill down query concepts.
select stores.store_name, dates.qtr quarter, sum(sales.total_sale) store_quarter_sales from stores, sales, dates where
  stores.store_id = sales.store_id and
  sales.date_id = dates.date_id
  group by stores.store_name, dates.qtr
  order by store_name, qtr
  ;
  
--5, Create a materialised view with name "STOREANALYSIS" that present the product-wise sales analysis for each store.
DROP materialized view STOREANALYSIS;
create materialized view STOREANALYSIS as

  select stores.store_name, products.product_name, sum(sales.total_sale) store_quarter_sales from stores, sales, products where
  stores.store_id = sales.store_id and
  sales.product_id = products.product_id
  group by stores.store_name, products.product_name
  order by store_name, product_name
;

commit; 
