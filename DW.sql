create database DW
go
create schema dbo
go

create schema dw
go
create function dbo.MIN_DATE() returns date as
begin
    return cast('2016-1-1' as date)
end
create table dw.date
(
    TimeKey                     int,
    FullDateAlternateKey        varchar(max),
    PersianFullDateAlternateKey varchar(max),
    DayNumberOfWeek             int,
    PersianDayNumberOfWeek      int,
    EnglishDayNameOfWeek        varchar(max),
    PersianDayNameOfWeek        nvarchar(max),
    DayNumberOfMonth            int,
    PersianDayNumberOfMonth     int,
    DayNumberOfYear             int,
    PersianDayNumberOfYear      int,
    WeekNumberOfYear            int,
    PersianWeekNumberOfYear     int,
    EnglishMonthName            varchar(max),
    PersianMonthName            varchar(max),
    MonthNumberOfYear           int,
    PersianMonthNumberOfYear    int,
    CalendarQuarter             int,
    PersianCalendarQuarter      int,
    CalendarYear                int,
    PersianCalendarYear         int,
    CalendarSemester            int,
    PersianCalendarSemester     int,
    dateKey                     date
)
go

create table dw.dim_customer
(
    customer_id int,
    first_name  varchar(255),
    last_name   varchar(255),
    phone       varchar(25),
    email       varchar(255),
    street      varchar(255),
    city        varchar(50),
    state       varchar(25),
    zip_code    varchar(5)
)
go

create table dw.dim_gift
(
    id        int,
    gift_code varchar(50),
    amount    numeric(18),
    count     int
)
go

create table dw.dim_product
(
    product_code   int,
    product_src_id int,
    product_name   varchar(255),
    brand_id       int,
    brand_name     varchar(255),
    category_id    int,
    category_name  varchar(255),
    model_year     smallint,
    list_price     decimal(10, 2),
    start_date     date,
    finish_date    date,
    current_flag   bit
)
go

create table dw.dim_service_type
(
    id          int,
    type        varchar(50),
    description varchar(500)
)
go

create table dw.dim_staff
(
    staff_id           int,
    first_name         varchar(50),
    last_name          varchar(50),
    email              varchar(255),
    active             tinyint,
    store_id           int,
    manager_id         int,
    manager_first_name varchar(50),
    manager_last_name  varchar(50),
    original_phone     varchar(25),
    effective_date     date,
    current_phone      varchar(25)
)
go

create table dw.dim_store
(
    store_id   int,
    store_name varchar(255),
    phone      varchar(25),
    email      varchar(255),
    street     varchar(255),
    city       varchar(255),
    state      varchar(10),
    zip_code   varchar(5)
)
go

create table dw.fact_customer_gift_card_factless
(
    customer_id int,
    gift_id     int,
    date        date
)
go

create table dw.fact_order_transactional
(
    store_id                     int,
    staff_id                     int,
    customer_id                  int,
    product_code                 int,
    ordered_date                 date,
    required_date                date,
    product_count                int,
    item_income_without_discount decimal(10, 2),
    discount_rate                decimal(4, 2),
    discount_amount              decimal(10, 2),
    sale_income_with_discount    decimal(10, 2)
)
go

create table dw.fact_product_daily
(
    store_id       int,
    product_code   int,
    date           date,
    stock_quantity int,
    sale_count     int,
    sale_income    decimal(20, 2)
)
go

create index date_index
    on dw.fact_product_daily (date)
go

create table dw.fact_service_transactional
(
    customer_id  int,
    date         date,
    service_type int,
    service_cost numeric(18)
)
go

create index date_index
    on dw.fact_service_transactional (date)
go

create table dw.fact_staff_sale_accumulative
(
    staff_id                     int,
    total_sale                   decimal(20, 2),
    total_sold_item_count        int,
    total_registered_order_count int,
    avg_discount_rate            decimal(4, 2)
)
go

create table dw.logs
(
    date       datetime,
    table_name varchar(50),
    status     tinyint,
    text       varchar(500)
)
go

create table temp_product
(
    product_code   int,
    product_src_id int,
    product_name   varchar(255),
    brand_id       int,
    brand_name     varchar(255),
    category_id    int,
    category_name  varchar(255),
    model_year     smallint,
    list_price     decimal(10, 2),
    start_date     date,
    finish_date    date,
    current_flag   bit
)
go

create table temp_product_changed
(
    product_code   int,
    product_src_id int,
    product_name   varchar(255),
    brand_id       int,
    brand_name     varchar(255),
    category_id    int,
    category_name  varchar(255),
    model_year     smallint,
    list_price     decimal(10, 2),
    start_date     date,
    finish_date    date,
    current_flag   bit
)
go

create table temp_product_order
(
    store_id     int,
    product_code int,
    sale_count   int,
    sale_income  decimal(20, 2)
)
go

create table temp_product_store_stock
(
    store_id       int,
    product_code   int,
    stock_quantity int
)
go

create table temp_staff_acc
(
    staff_id                     int,
    total_sale                   decimal(20, 2),
    total_sold_item_count        int,
    total_registered_order_count int,
    avg_discount_rate            decimal(4, 2)
)
go

create procedure dw.insert_fact_customer_gift_card_factless @end_date date as
begin
    insert into dw.logs
    values (current_timestamp, 'fact_customer_gift_card_factless', 2, 'insert_fact_customer_gift_card_factless')
    declare @cur_date as date

    select @cur_date = IIF(max(date) is null, dbo.MIN_DATE(), dateadd(day, 1, max(date)))
    from dw.fact_customer_gift_card_factless
    while @cur_date < @end_date
        begin
            insert into dw.fact_customer_gift_card_factless
            select card, customer, date
            from SA.sa.gift_card_usages
            where date = @cur_date

            set @cur_date = dateadd(day, 1, @cur_date)
        end
    insert into dw.logs
    values (current_timestamp, 'fact_customer_gift_card_factless', 3, 'insert_fact_customer_gift_card_factless')
end
go

create procedure dw.insert_fact_order_transactional @end_date date as
begin
    declare @cur_date as date
    insert into dw.logs
    values (current_timestamp, 'fact_order_transactional', 2, 'insert_fact_order_transactional ')
    select @cur_date = IIF(max(ordered_date) is null, dbo.MIN_DATE(), dateadd(day, 1, max(ordered_date)))
    from dw.fact_order_transactional
    while @cur_date < @end_date
        begin
            insert into dw.fact_order_transactional
            select store_id,
                   staff_id,
                   customer_id,
                   product_code,
                   order_date,
                   required_date,
                   quantity,
                   oi.list_price * quantity,
                   discount,
                   discount * (oi.list_price * quantity),
                   oi.list_price * quantity - discount * (oi.list_price * quantity)
            from SA.sa.orders o
                     full outer join SA.sa.order_items oi
                                     on o.order_id = oi.order_id and o.order_id is not null and oi.order_id is not null
                     left join dw.dim_product dp on oi.product_id = dp.product_src_id and current_flag = 1
            where order_date = @cur_date
            set @cur_date = dateadd(day, 1, @cur_date)
        end
    insert into dw.logs
    values (current_timestamp, 'fact_order_transactional', 3, 'insert_fact_order_transactional ')
end
go

create procedure dw.insert_fact_product_daily @end_date date as
begin
    declare @cur_date as date
    if (select count(store_id) from temp_product_store_stock) != 0 or
       (select count(store_id) from temp_product_order) != 0
        begin
            print 'last attempt was not successful in fact_product_daily'
            return
        end
    select @cur_date = IIF(max(date) is null, dbo.MIN_DATE(), dateadd(day, 1, max(date)))
    from dw.fact_product_daily
    truncate table temp_product_store_stock
    truncate table temp_product_order

    insert into dw.logs
    values (current_timestamp, 'fact_product_daily', 2, ' start of procedure ')

    while @cur_date < @end_date
        begin

            insert into temp_product_store_stock
            select store_id,
                   product_code,
                   quantity
            from SA.sa.stocks s
                     left join dw.dim_product dp on s.product_id = dp.product_src_id and current_flag = 1

            insert into temp_product_order
            select store_id,
                   product_code,
                   sum(quantity),
                   sum(oi.list_price * quantity - discount * (oi.list_price * quantity))
            from SA.sa.orders o
                     full outer join SA.sa.order_items oi
                                     on o.order_id = oi.order_id and o.order_id is not null and oi.order_id is not null
                     left join dw.dim_product dp on oi.product_id = dp.product_src_id and current_flag = 1
            where order_date = @cur_date
            group by store_id, product_code

            insert into dw.fact_product_daily
            select tpss.store_id,
                   tpss.product_code,
                   @cur_date,
                   stock_quantity,
                   isnull(sale_count, 0),
                   isnull(sale_income, 0.0)
            from temp_product_store_stock tpss
                     left join temp_product_order tpo
                               on tpss.store_id = tpo.store_id and tpss.product_code = tpo.product_code

            delete from temp_product_store_stock where 1 > 0
            delete from temp_product_order where 1 > 0
            set @cur_date = dateadd(day, 1, @cur_date)
        end

    insert into dw.logs
    values (current_timestamp, 'fact_product_daily', 3, ' end of procedure ')

end
go

create procedure dw.insert_fact_service_transactional @end_date date as
begin
    insert into dw.logs
    values (current_timestamp, 'fact_service_transactional', 2, 'insert_fact_service_transactional')
    declare @cur_date as date

    select @cur_date = IIF(max(date) is null, dbo.MIN_DATE(), dateadd(day, 1, max(date)))
    from dw.fact_service_transactional

    while @cur_date < @end_date
        begin
            insert into dw.fact_service_transactional
            select customer_id, date, type, cost
            from SA.sa.customer_services
            where date = @cur_date
            set @cur_date = dateadd(day, 1, @cur_date)
        end

    insert into dw.logs
    values (current_timestamp, 'fact_service_transactional', 3, 'insert_fact_service_transactional')
end
go

create procedure dw.insert_fact_staff_sale_accumulative @end_date date as
begin
    declare @cur_date as date
    declare @last_run_status as tinyint

    select top 1 @cur_date = date, @last_run_status = status
    from dw.logs
    where table_name = 'fact_staff_sale_accumulative'
    order by date desc, status desc

    if @last_run_status != 3 and @last_run_status is not null
        begin
            print 'last attempt was not successful in fact_staff_sale_accumulative'
            return
        end
    if @last_run_status is null
        set @cur_date = dbo.MIN_DATE()


    delete from temp_staff_acc where 1 > 0

    while @cur_date < @end_date
        begin

            insert into temp_staff_acc
            select staff_id,
                   sum(oi.list_price * quantity - discount * (oi.list_price * quantity)),
                   sum(quantity),
                   count(o.order_id),
                   avg(discount)
            from SA.sa.orders o
                     full outer join SA.sa.order_items oi
                                     on o.order_id = oi.order_id and o.order_id is not null and oi.order_id is not null
            where o.order_date = @cur_date
            group by staff_id

            set @cur_date = dateadd(day, 1, @cur_date)
        end
    insert into temp_staff_acc
    select staff_id, total_sale, total_sold_item_count, total_registered_order_count, avg_discount_rate
    from dw.fact_staff_sale_accumulative


    insert into dw.logs
    values (@end_date, 'fact_staff_sale_accumulative', 2, 'start deleting and replace table ')

    delete from dw.fact_staff_sale_accumulative where 1 > 0

    insert into dw.fact_staff_sale_accumulative
    select staff_id,
           sum(total_sale),
           sum(total_sold_item_count),
           sum(total_registered_order_count),
           avg(avg_discount_rate * total_registered_order_count) / sum(total_registered_order_count)
    from temp_staff_acc
    group by staff_id
    insert into dw.logs
    values (@end_date, 'fact_staff_sale_accumulative', 3, 'finish deleting and replace table ')

end
go

create procedure dw.insert_or_update_dim_customer as
begin
    insert into dw.logs
    values (current_timestamp, 'dim_customer ', 2, ' insert_or_update_dim_customer ')

    SELECT customer_id,
           first_name,
           last_name,
           phone,
           email,
           street,
           city,
           state,
           zip_code
    INTO temp_customer
    FROM dw.dim_customer
    where 1 = 0

    delete from temp_customer where 1 > 0

--     insert new rows which they are not in dim

    insert into temp_customer
    select customer_id,
           first_name,
           last_name,
           phone,
           email,
           street,
           city,
           state,
           zip_code
    from SA.sa.customers
    where customer_id not in (select customer_id from dw.dim_customer)

--     insert old dim rows and update  if it is necessary

    insert into temp_customer
    select dc.customer_id,
           dc.first_name,
           dc.last_name,
           IIF(c.customer_id is null, dc.phone, c.phone),
           IIF(c.customer_id is null, dc.email, c.email),
           IIF(c.customer_id is null, dc.street, c.street),
           IIF(c.customer_id is null, dc.city, c.city),
           IIF(c.customer_id is null, dc.state, c.state),
           IIF(c.customer_id is null, dc.zip_code, c.zip_code)
    from dw.dim_customer dc
             left join SA.sa.customers c on dc.customer_id = c.customer_id

    delete from dw.dim_customer where 1 > 0

    insert into dw.dim_customer
    select customer_id,
           first_name,
           last_name,
           phone,
           email,
           street,
           city,
           state,
           zip_code
    from temp_customer

    drop table temp_customer

    insert into dw.logs
    values (current_timestamp, 'dim_customer ', 3, ' insert_or_update_dim_customer ')
end
go

create procedure dw.insert_or_update_dim_gift as
begin

    insert into dw.logs
    values (current_timestamp, 'dim_gift', 2, 'insert_or_update_dim_gift ')

    select id, gift_code, amount, count
    into temp_gift
    from dw.dim_gift
    where 1 = 0

    delete from temp_gift where 1 > 0

--     insert new rows which they are not in dim

    insert into temp_gift
    select id, gift_code, amount, count
    from SA.sa.gift_cards
    where id not in (select dim_gift.id from dw.dim_gift)

--     insert old dim rows

    insert into temp_gift
    select dc.id,
           dc.gift_code,
           dc.amount,
           dc.count
    from dw.dim_gift dc
             left join SA.sa.gift_cards c on dc.id = c.id

    delete from dw.dim_gift where 1 > 0

    insert into dw.dim_gift
    select id, gift_code, amount, count
    from temp_gift

    drop table temp_gift
    insert into dw.logs
    values (current_timestamp, 'dim_gift', 3, 'insert_or_update_dim_gift ')
end
go

create procedure dw.insert_or_update_dim_product as
begin
    declare @last_run_status tinyint
    select top 1 @last_run_status = status
    from dw.logs
    where table_name = 'dim_product'
    order by date desc, status desc

    if @last_run_status != 3 and @last_run_status is not null
        begin
            print 'last attempt was not successful in dim_product'
            return
        end


    delete from temp_product where 1 > 0
    delete from temp_product_changed where 1 > 0

--     insert deleted, not changed and not currents rows from dim
    insert into temp_product
    select dp.product_code,
           dp.product_src_id,
           dp.product_name,
           dp.brand_id,
           dp.brand_name,
           dp.category_id,
           dp.category_name,
           dp.model_year,
           dp.list_price,
           start_date,
           IIF(p.product_id is null and dp.current_flag != 0, current_timestamp, dp.finish_date),
           IIF(p.product_id is null and dp.current_flag != 0, 0, dp.current_flag)
    from dw.dim_product dp
             left join SA.sa.products p on dp.product_src_id = p.product_id
    where p.product_id is null
       or dp.current_flag = 0
       or (dp.current_flag = 1 and dp.list_price = p.list_price)

--     insert changed from dim
    insert into temp_product_changed
    select product_code,
           product_src_id,
           product_name,
           brand_id,
           brand_name,
           category_id,
           category_name,
           model_year,
           list_price,
           start_date,
           current_timestamp,
           0
    from dw.dim_product
    where product_code not in (select product_code from temp_product)

--     insert changed rows from temp changed
    insert into temp_product
    select product_code,
           product_src_id,
           product_name,
           brand_id,
           brand_name,
           category_id,
           category_name,
           model_year,
           list_price,
           start_date,
           finish_date,
           current_flag
    from temp_product_changed

--     insert changed current rows and new ones from src
    insert into temp_product(product_code, product_src_id, product_name, brand_id, brand_name, category_id,
                             category_name, model_year, list_price,
                             start_date, finish_date, current_flag)
    select (select isnull(max(product_code), 0) from temp_product) + row_number() over ( order by product_id ),
           product_id,
           product_name,
           p.brand_id,
           brand_name,
           p.category_id,
           category_name,
           model_year,
           list_price,
           current_timestamp,
           null,
           1
    from SA.sa.products p
             join SA.sa.brands on p.brand_id = brands.brand_id
             join SA.sa.categories on p.category_id = categories.category_id
    where product_id in (select product_src_id from temp_product_changed)
       or product_id not in (select product_src_id from temp_product)


    insert into dw.logs
    values (current_timestamp, 'dim_product', 2, 'start deleting and replace table ')


    delete from dw.dim_product where 1 > 0

    insert into dw.dim_product(product_code, product_src_id, product_name, brand_id, brand_name, category_id,
                               category_name, model_year,
                               list_price, start_date, finish_date, current_flag)
    select product_code,
           product_src_id,
           product_name,
           brand_id,
           brand_name,
           category_id,
           category_name,
           model_year,
           list_price,
           start_date,
           finish_date,
           current_flag
    from temp_product

    insert into dw.logs
    values (current_timestamp, 'dim_product', 3, 'finish deleting and replace table ')


end
go

create procedure dw.insert_or_update_dim_service_type as
begin

    insert into dw.logs
    values (current_timestamp, 'dim_service_type', 2, 'insert_or_update_dim_service_type')
    select id, type, description
    into temp_service_type
    from dw.dim_service_type
    where 1 = 0


    delete from temp_service_type where 1 > 0

--     insert new rows which they are not in dim

    insert into temp_service_type
    select id, type, description
    from SA.sa.customer_service_type
    where id not in (select dim_service_type.id from dw.dim_service_type)

--     insert old dim rows and update  if it is necessary

    insert into temp_service_type
    select dc.id,
           dc.type,
           IIF(c.id is null, dc.description, c.description)
    from dw.dim_service_type dc
             left join SA.sa.customer_service_type c on dc.id = c.id

    delete from dw.dim_service_type where 1 > 0

    insert into dw.dim_service_type
    select id, type, description
    from temp_service_type

    drop table temp_service_type
    insert into dw.logs
    values (current_timestamp, 'dim_service_type', 3, 'insert_or_update_dim_service_type')
end
go

create procedure dw.insert_or_update_dim_staff as
begin

    insert into dw.logs
    values (current_timestamp, 'dim_staff', 2, 'insert_or_update_dim_staff ')

    select staff_id,
           first_name,
           last_name,
           email,
           active,
           store_id,
           manager_id,
           manager_first_name,
           manager_last_name,
           original_phone,
           effective_date,
           current_phone
    into temp_staff
    from dw.dim_staff
    where 1 = 0

    delete from temp_staff where 1 > 0

--     insert new rows which they are not in dim

    insert into temp_staff
    select s.staff_id,
           s.first_name,
           s.last_name,
           s.email,
           s.active,
           s.store_id,
           s.manager_id,
           m.first_name,
           m.last_name,
           null,
           current_timestamp,
           s.phone
    from SA.sa.staffs s
             left join SA.sa.staffs m on s.manager_id = m.staff_id
    where s.staff_id not in (select dim_staff.staff_id from dw.dim_staff)

--     insert old dim rows and update  if it is necessary

    insert into temp_staff
    select ds.staff_id,
           ds.first_name,
           ds.last_name,
           ds.email,
           ds.active,
           ds.store_id,
           ds.manager_id,
           manager_first_name,
           manager_last_name,
           IIF(ds.current_phone = s.phone or (ds.current_phone is null and s.phone is null), ds.original_phone,
               ds.current_phone),
           IIF(ds.current_phone = s.phone or (ds.current_phone is null and s.phone is null), ds.effective_date,
               current_timestamp),
           IIF(ds.current_phone = s.phone or (ds.current_phone is null and s.phone is null), ds.current_phone, s.phone)
    from dw.dim_staff ds
             left join SA.sa.staffs s on ds.staff_id = s.staff_id
             left join SA.sa.staffs m on s.manager_id = m.staff_id

    delete from dw.dim_staff where 1 > 0

    insert into dw.dim_staff
    select staff_id,
           first_name,
           last_name,
           email,
           active,
           store_id,
           manager_id,
           manager_first_name,
           manager_last_name,
           original_phone,
           effective_date,
           current_phone
    from temp_staff

    drop table temp_staff
    insert into dw.logs
    values (current_timestamp, 'dim_staff', 3, 'insert_or_update_dim_staff ')
end
go

create procedure dw.insert_or_update_dim_store as
begin

    insert into dw.logs
    values (current_timestamp, 'dim_store', 2, 'insert_or_update_dim_store ')
    select store_id,
           store_name,
           phone,
           email,
           street,
           city,
           state,
           zip_code
    into temp_store
    from dw.dim_store
    where 1 = 0

    delete from temp_store where 1 > 0

--     insert new rows which they are not in dim

    insert into temp_store
    select store_id,
           store_name,
           phone,
           email,
           street,
           city,
           state,
           zip_code
    from SA.sa.stores
    where store_id not in (select dim_store.store_id from dw.dim_store)

--     insert old dim rows and update  if it is necessary

    insert into temp_store
    select dc.store_id,
           dc.store_name,
           IIF(c.store_id is null, dc.phone, c.phone),
           IIF(c.store_id is null, dc.email, c.email),
           IIF(c.store_id is null, dc.street, c.street),
           IIF(c.store_id is null, dc.city, c.city),
           IIF(c.store_id is null, dc.state, c.state),
           IIF(c.store_id is null, dc.zip_code, c.zip_code)
    from dw.dim_store dc
             left join SA.sa.stores c on dc.store_id = c.store_id

    delete from dw.dim_store where 1 > 0

    insert into dw.dim_store
    select store_id,
           store_name,
           phone,
           email,
           street,
           city,
           state,
           zip_code
    from temp_store

    drop table temp_store
    insert into dw.logs
    values (current_timestamp, 'dim_store', 3, 'insert_or_update_dim_store ')
end
go

create procedure dw.reset_dw as
begin
    truncate table DW.dw.dim_customer
    truncate table DW.dw.dim_gift
    truncate table DW.dw.dim_product
    truncate table DW.dw.dim_service_type
    truncate table DW.dw.dim_staff
    truncate table DW.dw.dim_store
    truncate table DW.dw.fact_customer_gift_card_factless
    truncate table DW.dw.fact_order_transactional
    truncate table DW.dw.fact_product_daily
    truncate table DW.dw.fact_service_transactional
    truncate table DW.dw.fact_staff_sale_accumulative
    truncate table DW.dw.logs

end
go

create procedure dw.runner @end_date date as
begin
    exec SA.sa.update_sa @end_date
    exec dw.insert_or_update_dim_customer
    exec dw.insert_or_update_dim_product
    exec dw.insert_or_update_dim_staff
    exec dw.insert_or_update_dim_store
    exec dw.insert_or_update_dim_gift
    exec dw.insert_or_update_dim_service_type

    exec dw.insert_fact_order_transactional '2020-1-20'
    exec dw.insert_fact_service_transactional @end_date
    exec dw.insert_fact_product_daily @end_date
    exec dw.insert_fact_staff_sale_accumulative @end_date
    exec dw.insert_fact_customer_gift_card_factless @end_date
end
go

